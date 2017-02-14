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

from eventmq.log import setup_logger
from . import conf
from .constants import KBYE, STATUS
from .poller import Poller, POLLIN
from .sender import Sender
from .utils.classes import EMQPService, HeartbeatMixin
from .utils.devices import generate_device_name
from .utils.functions import get_timeout_from_headers
from .utils.messages import send_emqp_message as sendmsg
from .utils.settings import import_settings
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

    def __init__(self, *args, **kwargs):
        """
        .. note::

           All args are optional unless otherwise noted.

        Args:
            name (str): unique name of this instance. By default a uuid will be
                 generated.
            queues (tuple): List of queue names to listen on.
            skip_signal (bool): Don't register the signal handlers. Useful for
                 testing.
        """
        super(JobManager, self).__init__(*args, **kwargs)

        #: Define the name of this JobManager instance. Useful to know when
        #: referring to the logs.
        self.name = kwargs.pop('name', generate_device_name())
        logger.info('Initializing JobManager {}...'.format(self.name))

        #: keep track of workers
        concurrent_jobs = kwargs.pop('concurrent_jobs', None)
        if concurrent_jobs is not None:
            conf.CONCURRENT_JOBS = concurrent_jobs

        #: List of queues that this job manager is listening on
        self.queues = kwargs.pop('queues', None)

        if not kwargs.pop('skip_signal', False):
            # handle any sighups by reloading config
            signal.signal(signal.SIGHUP, self.sighup_handler)
            signal.signal(signal.SIGTERM, self.sigterm_handler)
            signal.signal(signal.SIGINT, self.sigterm_handler)
            signal.signal(signal.SIGQUIT, self.sigterm_handler)

        #: JobManager starts out by INFORMing the router of it's existence,
        #: then telling the router that it is READY. The reply will be the unit
        #: of work.
        # Despite the name, jobs are received on this socket
        self.outgoing = Sender(name=self.name)

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

            if events.get(self.outgoing) == POLLIN:
                msg = self.outgoing.recv_multipart()
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
                # Toggle default and failover worker_addr
                if (conf.WORKER_ADDR == conf.WORKER_ADDR_DEFAULT):
                    conf.WORKER_ADDR = conf.WORKER_ADDR_FAILOVER
                else:
                    conf.WORKER_ADDR = conf.WORKER_ADDR_DEFAULT

                break

    def on_request(self, msgid, msg):
        """
        Handles a REQUEST command

        Messages are formatted like this:
        [subcmd(str), {
            ...options...
        }]

        Subcommands:
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

        .. note:
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
        sendmsg(self.outgoing, 'READY')

    def send_reply(self, reply, msgid):
        """
         Sends an REPLY response

         Args:
             socket (socket): The socket to use for this ack
             recipient (str): The recipient id for the ack
             msgid: The unique id that we are acknowledging
         """
        sendmsg(self.outgoing, 'REPLY', [reply, msgid])

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
        sendmsg(self.outgoing, KBYE)
        self.outgoing.unbind(conf.WORKER_ADDR)
        super(JobManager, self).on_disconnect(msgid, msg)

    def on_kbye(self, msgid, msg):
        if not self.is_heartbeat_enabled:
            self.reset()

    def sighup_handler(self, signum, frame):
        logger.info('Caught signal %s' % signum)
        import_settings()
        import_settings(section='jobmanager')

        self.should_reset = True
        self.received_disconnect = True

    def sigterm_handler(self, signum, frame):
        logger.info('Shutting down..')
        sendmsg(self.outgoing, KBYE)

        self.awaiting_startup_ack = False
        self.received_disconnect = True

    def jobmanager_main(self, broker_addr=None):
        """
        Kick off jobmanager with logging and settings import

        Args:
            broker_addr (str): The address of the broker to connect to.
        """
        setup_logger('')
        import_settings()
        import_settings(section='jobmanager')

        # If this manager was passed explicit options, favor those
        if self.queues:
            conf.QUEUES = self.queues

        if broker_addr:
            conf.WORKER_ADDR = broker_addr

        self.start(addr=conf.WORKER_ADDR, queues=conf.QUEUES)


def jobmanager_main():
    j = JobManager()
    j.jobmanager_main()
