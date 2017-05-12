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
from json import dumps as serializer, loads as deserializer

import logging
from multiprocessing import Queue as mp_queue
import os
import signal
import sys
import time

import zmq

from .constants import KBYE, STATUS
from .poller import Poller, POLLIN
from .sender import Sender
from .settings import conf, reload_settings
from .utils.classes import EMQPService, HeartbeatMixin
from .utils.devices import generate_device_name
from .utils.functions import get_timeout_from_headers
from .utils.messages import send_emqp_message as sendmsg
from .utils.timeutils import monotonic
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

        reload_settings('jobmanager', self.override_settings)

        super(JobManager, self).__init__(*args, **kwargs)

        #: Define the name of this JobManager instance. Useful to know when
        #: referring to the logs.
        if conf.NAME:
            self.name = "{}:{}".format(conf.NAME, generate_device_name())
        else:
            self.name = generate_device_name()

        logger.info('Initializing JobManager {}...'.format(self.name))

        if not skip_signal:
            # handle any sighups by reloading config
            signal.signal(signal.SIGHUP, self.sighup_handler)
            signal.signal(signal.SIGTERM, self.sigterm_handler)
            signal.signal(signal.SIGINT, self.sigterm_handler)
            signal.signal(signal.SIGQUIT, self.sigterm_handler)
            signal.signal(signal.SIGUSR1, self.handle_pdb)

        #: JobManager starts out by INFORMing the router of it's existence,
        #: then telling the router that it is READY. The reply will be the unit
        #: of work.
        # Despite the name, jobs are received on this socket
        self.frontend = Sender(name=self.name)

        self.poller = Poller()

        #: Stats and monitoring information

        #: Jobs in flight tracks all jobs currently executing.
        #: Key: msgid, Value: The message with all the details of the job
        self.jobs_in_flight = {}

        #: Running total number of REQUEST messages received on the broker
        self.total_requests = 0
        #: Running total number of READY messages sent to the broker
        self.total_ready_sent = 0
        #: Keep track of what pids are servicing our requests
        #: Key: pid, Value: # of jobs completed on the process with that pid
        self.pid_distribution = {}

        #: Setup worker queues
        self.request_queue = mp_queue()
        self.finished_queue = mp_queue()
        self._setup()

    def handle_pdb(self, sig, frame):
        import pdb
        pdb.Pdb().set_trace(frame)

    @property
    def workers(self):
        if not hasattr(self, '_workers'):
            self._workers = {}
            for i in range(0, conf.CONCURRENT_JOBS):
                w = Worker(self.request_queue, self.finished_queue,
                           os.getpid())
                w.start()
                self._workers[w.pid] = w

        return self._workers.values()

    def _start_event_loop(self):
        """
        Starts the actual event loop. Usually called by :meth:`start`
        """
        # Acknowledgment has come
        # Send a READY for each available worker

        # Instatiate workers
        self.workers

        self.status = STATUS.running

        try:
            while True:
                # Clear any workers if it's time to shut down
                if self.received_disconnect and self.status != STATUS.stopping:
                    self.status = STATUS.stopping
                    for _ in range(len(self.workers)):
                        logger.debug('Requesting worker death')
                        self.request_queue.put_nowait('DONE')

                    self.disconnect_time = monotonic()
                else:
                    # Call appropiate callbacks for each finished job
                    while True:
                        try:
                            resp = self.finished_queue.get_nowait()
                        except Queue.Empty:
                            break
                        else:
                            self.handle_response(resp)

                    if self.status == STATUS.stopping:
                        if len(self._workers) > 0:
                            time.sleep(0.1)
                        elif not self.should_reset:
                            sys.exit(0)
                        else:
                            break

                        if monotonic() > self.disconnect_time + \
                           conf.KILL_GRACE_PERIOD:
                            logger.debug("Killing unresponsive workers")
                            for pid in self._workers.keys():
                                self.kill_worker(pid, signal.SIGKILL)

                            if not self.should_reset:
                                sys.exit(0)
                            else:
                                break
                    else:
                        try:
                            events = self.poller.poll(1000)
                        except zmq.ZMQError:
                            logger.debug('Disconnecting due to ZMQError while'
                                         ' polling')
                            sendmsg(self.outgoing, KBYE)
                            self.received_disconnect = True
                            continue

                        if events.get(self.frontend) == POLLIN:
                            msg = self.frontend.recv_multipart()
                            self.process_message(msg)

                        # Note: `maybe_send_heartbeat` is mocked by the tests
                        # to return False, so it should stay at the bottom of
                        # the loop
                        if not self.maybe_send_heartbeat(events):
                            break

        except Exception:
            logger.exception("Unhandled exception in main jobmanager loop")

        # Cleanup
        del self._workers

        # Flush the queues with workers
        self.request_queue = mp_queue()
        self.finished_queue = mp_queue()
        logger.info("Reached end of event loop")

    def handle_response(self, resp):
        """
        Handles a response from a worker process to the jobmanager

        Args:
          resp (dict): Must contain a key 'callback' with the desired callback
        function as a string, i.e. 'worker_done' which is then called

        Sample Input
        resp = {
            'callback': 'worker_done', (str)
            'msgid': 'some_uuid', (str)
            'return': 'return value', (dict)
            'pid': 'pid_of_worker_process' (int)
        }

        return_value must be a dictionary that can be json serialized and
        formatted like:

        {
            "value": 'return value of job goes here'
        }

        if the 'return' value of resp is 'DEATH', the worker died so we clean
        that up as well

        """

        logger.debug(resp)
        pid = resp['pid']
        msgid = resp['msgid']
        callback = resp['callback']
        death = resp['death']

        callback = getattr(self, callback)
        callback(resp['return'], msgid, death, pid)

        if msgid in self.jobs_in_flight:
            del self.jobs_in_flight[msgid]

        if not death:
            if pid not in self.pid_distribution:
                self.pid_distribution[pid] = 1
            else:
                self.pid_distribution[pid] += 1

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

        self.total_requests += 1

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

        self.jobs_in_flight[msgid] = (monotonic(), payload)

        self.request_queue.put(payload)

    def premature_death(self, reply, msgid):
        """
        Worker died before running any jobs
        """
        return

    def worker_death(self, reply, msgid, death, pid):
        """
        Worker died of natural causes, ensure its death and
        remove from tracking, will be replaced on next heartbeat
        """
        if pid in self._workers.keys():
            del self._workers[pid]

    def worker_ready(self, reply, msgid, death, pid):
        if self.status != STATUS.stopping:
            self.send_ready()

    def worker_done_with_reply(self, reply, msgid, death, pid):
        """
        Worker finished a job and requested the return value
        """
        try:
            reply = serializer(reply)
        except TypeError as e:
            reply = serializer({"value": str(e)})

        self.send_reply(reply, msgid)

        if self.status != STATUS.stopping and not death:
            self.send_ready()

    def worker_done(self, reply, msgid, death, pid):
        """
        Worker finished a job, notify broker of an additional slot opening
        """
        if self.status != STATUS.stopping and not death:
            self.send_ready()

    def send_ready(self):
        """
        send the READY command upstream to indicate that JobManager is ready
        for another REQUEST message.
        """
        self.total_ready_sent += 1
        logger.debug("Sending READY")
        sendmsg(self.frontend, 'READY')

    def send_reply(self, reply, msgid):
        """
         Sends an REPLY response

         Args:
             reply: Message to send as the reply
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
        # Kill workers that aren't alive
        logger.debug("Jobs in flight: {}".format(len(self.jobs_in_flight)))
        logger.debug("Total requests: {}".format(self.total_requests))
        logger.debug("Total ready sent: {}".format(self.total_ready_sent))
        try:
            [self.kill_worker(w.pid, signal.SIGKILL) for w in self.workers
             if not w.is_alive]
        except Exception as e:
            logger.warning(str(e))

        self._workers = {w.pid: w for w in self.workers
                         if w.is_alive()}

        if len(self._workers) < conf.CONCURRENT_JOBS:
            logger.warning("{} worker process(es) may have died...recreating"
                           .format(conf.CONCURRENT_JOBS - len(self.workers)))

        for i in range(0, conf.CONCURRENT_JOBS - len(self.workers)):
            w = Worker(self.request_queue, self.finished_queue, os.getpid())
            w.start()
            self._workers[w.pid] = w

    def kill_worker(self, pid, signal):
        try:
            os.kill(pid, signal)
        except Exception as e:
            logger.exception(
                "Encountered exception trying to send {} to worker {}: {}"
                .format(signal, pid, str(e)))

    def on_disconnect(self, msgid, msg):
        sendmsg(self.frontend, KBYE)
        self.frontend.unbind(conf.CONNECT_ADDR)
        super(JobManager, self).on_disconnect(msgid, msg)

    def on_kbye(self, msgid, msg):
        if not self.is_heartbeat_enabled:
            self.reset()

    def sighup_handler(self, signum, frame):
        logger.info('Caught SIGHUP')
        reload_settings('jobmanager', self.override_settings)

        self.should_reset = True
        self.received_disconnect = True

    def sigterm_handler(self, signum, frame):
        if not self.received_disconnect:
            logger.info('Shutting down..')
            sendmsg(self.frontend, KBYE)

            self.awaiting_startup_ack = False
            self.received_disconnect = True
