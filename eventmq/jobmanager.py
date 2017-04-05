# This file is part of eventmq.
#
# eventmq is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 2.1 of the License, or (at your option)
# any later version.
#
# eventmq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with eventmq.  If not, see <http://www.gnu.org/licenses/>.
"""
:mod:`jobmanager` -- Job Manager
================================
Ensures things about jobs and spawns the actual tasks
"""
from json import loads as deserializer
import logging
from multiprocessing import Queue as mp_queue
import signal
import sys

import zmq

from .constants import KBYE, STATUS
from .poller import Poller, POLLIN
from .sender import Sender
from .settings import conf, load_settings_from_dict, load_settings_from_file
from .utils.classes import EMQPService, HeartbeatMixin
from .utils.devices import generate_device_name
from .utils.functions import get_timeout_from_headers
from .utils.messages import send_emqp_message as sendmsg
from .worker import MultiprocessWorker as Worker


if sys.version[0] == '2':
    import Queue
else:
    import queue as Queue


logger = logging.getLogger(__name__)


class JobManager(HeartbeatMixin, EMQPService):
    """
    The exposed portion of the worker. The job manager's main responsibility is
    to manage the resources on the server it's running.

    This job manager uses multiprocessing Queues
    """
    SERVICE_TYPE = 'worker'

    def __init__(self, override_settings=None, skip_signal=False, *args,
                 **kwargs):
        """
        Initialize the job manager. Loads settings, creates sockets, loads them
        into the poller and generally prepares the service for a ``start()``
        call.

        Args:
            override_settings (dict): Dictionary containing settings that will
                override defaults and anything loaded from a config file. The
                key should match the upper case conf setting name.
                See: :func:`eventmq.settings.load_settings_from_dict`
            skip_signal (bool): Don't register the signal handlers. Useful for
                testing.
        """
        self.override_settings = override_settings

        self.load_settings()

        super(JobManager, self).__init__(*args, **kwargs)

        #: Define the name of this JobManager instance. Useful to know when
        #: referring to the logs.
        self.name = conf.NAME or generate_device_name()
        logger.info('Initializing JobManager {}...'.format(self.name))

        if skip_signal:
            # handle any sighups by reloading config
            signal.signal(signal.SIGHUP, self.sighup_handler)
            signal.signal(signal.SIGTERM, self.sigterm_handler)
            signal.signal(signal.SIGINT, self.sigterm_handler)
            signal.signal(signal.SIGQUIT, self.sigterm_handler)

        #: JobManager starts out by INFORMing the router of it's existence,
        #: then telling the router that it is READY. The reply will be the unit
        #: of work.
        # Despite the name, jobs are received on this socket
        self.frontend = Sender(name=self.name)

        self.poller = Poller()

        #: Setup worker queues
        self.request_queue = mp_queue()
        self.finished_queue = mp_queue()
        self._setup()

    @property
    def workers(self):
        if not hasattr(self, '_workers'):
            self._workers = []
            for i in range(0, conf.CONCURRENT_JOBS):
                w = Worker(self.request_queue, self.finished_queue)
                w.start()
                self._workers.append(w)

        return self._workers

    def _start_event_loop(self):
        """
        Starts the actual event loop. Usually called by :meth:`start`
        """
        # Acknowledgment has come
        # Send a READY for each available worker

        for i in range(0, len(self.workers)):
            self.send_ready()

        self.status = STATUS.running

        while True:
            # Clear any workers if it's time to shut down
            if self.received_disconnect:
                self.status = STATUS.stopping
                for _ in range(0, len(self.workers)):
                    logger.debug('Requesting worker death...')
                    self.request_queue.put_nowait('DONE')
                self.request_queue.close()
                self.request_queue.join_thread()

                for w in self.workers:
                    w.join()

                break

            try:
                events = self.poller.poll()
            except zmq.ZMQError:
                logger.debug('Disconnecting due to ZMQ Error while polling')
                self.received_disconnect = True
                continue

            if events.get(self.frontend) == POLLIN:
                msg = self.frontend.recv_multipart()
                self.process_message(msg)

            # Call appropiate callbacks for each finished job
            while True:
                try:
                    resp = self.finished_queue.get_nowait()
                except Queue.Empty:
                    break
                else:
                    if resp['callback'] == 'worker_done':
                        self.worker_done(resp['msgid'])
                    elif resp['callback'] == 'worker_done_with_reply':
                        self.worker_done_with_reply(resp['return'],
                                                    resp['msgid'])

            # Note: `maybe_send_heartbeat` is mocked by the tests to return
            #       False, so it should stay at the bottom of the loop.
            if not self.maybe_send_heartbeat(events):
                break

    def on_request(self, msgid, msg):
        """
        Handles a REQUEST command

        Messages are formatted like this:

        .. code::

           [subcmd(str), {
               ...options...
           }]

        Subcommands:

        .. code::

           run - run some callable. Options:
               {
                 'callable': func or method name (eg. walk),
                 'path': module path (eg. os.path),
                 'args': (optional) list of args,
                 'kwargs': (optional) dict of kwargs,
                 'class_args': (optional) list of args for class
                     instantiation,
                 'class_kwargs': (optional) dict of kwargs for class,
               }

        .. note::

           If you want to run a method from a class you must specify the class
           name in the path preceeded with a colon. 'name.of.mypacakge:MyClass'
        """
        # s_ indicates the string path vs the actual module and class
        # queue_name = msg[0]

        # Parse REQUEST message values
        headers = msg[1]
        payload = deserializer(msg[2])
        params = payload[1]

        if 'reply-requested' in headers:
            callback = 'worker_done_with_reply'
        else:
            callback = 'worker_done'

        timeout = get_timeout_from_headers(headers)

        payload = {}
        payload['params'] = params
        payload['timeout'] = timeout
        payload['msgid'] = msgid
        payload['callback'] = callback

        self.request_queue.put(payload)

    def worker_done_with_reply(self, reply, msgid):
        self.send_reply(reply, msgid)
        self.send_ready()

    def worker_done(self, msgid):
        self.send_ready()

    def send_ready(self):
        """
        send the READY command upstream to indicate that JobManager is ready
        for another REQUEST message.
        """
        sendmsg(self.frontend, 'READY')

    def send_reply(self, reply, msgid):
        """
         Sends an REPLY response

         Args:
             socket (socket): The socket to use for this ack
             recipient (str): The recipient id for the ack
             msgid: The unique id that we are acknowledging
         """
        sendmsg(self.frontend, 'REPLY', [reply, msgid])

    def on_heartbeat(self, msgid, message):
        """
        a placeholder for a noop command. The actual 'logic' for HEARTBEAT is
        in :meth:`self.process_message` as every message is counted as a
        HEARTBEAT
        """
        if self.status == STATUS.running:
            self.check_worker_health()

    def check_worker_health(self):
        """
        Checks for any dead processes in the pool and recreates them if
        necessary
        """
        self._workers = [w for w in self._workers if w.is_alive()]

        if len(self._workers) < conf.CONCURRENT_JOBS:
            logger.warning("{} worker process(es) may have died...recreating"
                           .format(conf.CONCURRENT_JOBS - len(self._workers)))

        for i in range(0, conf.CONCURRENT_JOBS - len(self._workers)):
            w = Worker(self.request_queue, self.finished_queue)
            w.start()
            self._workers.append(w)

    def on_disconnect(self, msgid, msg):
        sendmsg(self.frontend, KBYE)
        self.frontend.unbind(conf.CONNECT_ADDR)
        super(JobManager, self).on_disconnect(msgid, msg)

    def on_kbye(self, msgid, msg):
        if not self.is_heartbeat_enabled:
            self.reset()

    def sighup_handler(self, signum, frame):
        logger.info('Caught signal %s' % signum)
        self.load_settings()

        self.should_reset = True
        self.received_disconnect = True

    def sigterm_handler(self, signum, frame):
        logger.info('Shutting down..')
        sendmsg(self.frontend, KBYE)

        self.awaiting_startup_ack = False
        self.received_disconnect = True

    def load_settings(self):
        """
        Reload settings by resetting to defaults, reading the config, and
        setting any overridden settings.
        """
        conf.reload()
        load_settings_from_file('jobmanager')
        load_settings_from_dict(self.override_settings, 'jobmanager')
