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
from multiprocessing import Manager as MPManager
import os
import signal
import sys
import time

from six.moves import range
import zmq

from eventmq.log import setup_logger
from . import __version__
from . import conf
from .constants import KBYE, STATUS
from .poller import Poller, POLLIN
from .sender import Sender
from .utils.classes import EMQPService, HeartbeatMixin
from .utils.devices import generate_device_name
from .utils.functions import get_timeout_from_headers
from .utils.messages import send_emqp_message as sendmsg
from .utils.settings import import_settings
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

        setup_logger("eventmq")

        #: Define the name of this JobManager instance. Useful to know when
        #: referring to the logs.
        self.name = kwargs.pop('name', None) or generate_device_name()
        logger.info('EventMQ Version {}'.format(__version__))
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
            signal.signal(signal.SIGUSR1, self.handle_pdb)

        #: JobManager starts out by INFORMing the router of it's existence,
        #: then telling the router that it is READY. The reply will be the unit
        #: of work.
        # Despite the name, jobs are received on this socket
        self.outgoing = Sender(name=self.name)

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
        self._mp_manager = MPManager()
        self.request_queue = self._mp_manager.Queue()
        self.finished_queue = self._mp_manager.Queue()
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

        return list(self._workers.values())

    def _start_event_loop(self):
        """
        Starts the actual event loop. Usually called by :meth:`start`
        """
        # Acknowledgment has come
        # When the job manager unexpectedly disconnects from the router and
        # reconnects it needs to send a ready for each previously available
        # worker.
        # Send a READY for each previously available worker
        if hasattr(self, '_workers'):
            for _ in self._workers:
                self.send_ready()

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

                    if self.status == STATUS.stopping and \
                       not self.should_reset:
                        if len(self._workers) > 0:
                            time.sleep(0.1)
                        else:
                            sys.exit(0)

                        if monotonic() > self.disconnect_time + \
                           conf.KILL_GRACE_PERIOD:
                            logger.debug("Killing unresponsive workers")
                            for pid in self._workers.keys():
                                self.kill_worker(pid, signal.SIGKILL)
                            sys.exit(0)
                    else:
                        try:
                            events = self.poller.poll(1000)
                        except zmq.ZMQError as e:
                            logger.debug('Disconnecting due to ZMQError while'
                                         ' polling: {}'.format(e))
                            sendmsg(self.outgoing, KBYE)
                            self.received_disconnect = True
                            continue

                        if events.get(self.outgoing) == POLLIN:
                            msg = self.outgoing.recv_multipart()
                            self.process_message(msg)

                        # Note: `maybe_send_heartbeat` is mocked by the tests
                        # to return False, so it should stay at the bottom of
                        # the loop
                        if not self.maybe_send_heartbeat(events):
                            # Toggle default and failover worker_addr
                            if (conf.WORKER_ADDR == conf.WORKER_ADDR_DEFAULT):
                                conf.WORKER_ADDR = conf.WORKER_ADDR_FAILOVER
                            else:
                                conf.WORKER_ADDR = conf.WORKER_ADDR_DEFAULT

                            break

        except Exception:
            logger.exception("Unhandled exception in main jobmanager loop")

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

        if conf.SUPER_DEBUG:
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
        if pid in list(self._workers.keys()):
            del self._workers[pid]

    def worker_ready(self, reply, msgid, death, pid):
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
        sendmsg(self.outgoing, 'READY')

    def send_reply(self, reply, msgid):
        """
         Sends an REPLY response

         Args:
             reply: Message to send as the reply
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
        # Kill workers that aren't alive
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
        if not self.received_disconnect:
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
