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
from json import loads as serializer
import logging
import signal

from . import conf
from .poller import Poller, POLLIN
from .sender import Sender
from .utils.classes import EMQPService, HeartbeatMixin
from .utils.settings import import_settings
from .utils.devices import generate_device_name
from .utils.messages import send_emqp_message as sendmsg
from .worker import MultiprocessWorker as Worker
from eventmq.log import setup_logger
from multiprocessing import Queue as mp_queue
import Queue

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
        """
        super(JobManager, self).__init__(*args, **kwargs)

        #: Define the name of this JobManager instance. Useful to know when
        #: referring to the logs.
        self.name = kwargs.pop('name', generate_device_name())
        logger.info('Initializing JobManager {}...'.format(self.name))

        #: Setup worker queues
        self.request_queue = mp_queue()
        self.finished_queue = mp_queue()

        #: keep track of workers
        self.workers = []

        if not kwargs.pop('skip_signal', False):
            # handle any sighups by reloading config
            signal.signal(signal.SIGHUP, self.sighup_handler)

        #: JobManager starts out by INFORMing the router of it's existance,
        #: then telling the router that it is READY. The reply will be the unit
        #: of work.
        # Despite the name, jobs are received on this socket
        self.outgoing = Sender(name=self.name)

        self.poller = Poller()

        self._setup()

    def _start_event_loop(self):
        """
        Starts the actual eventloop. Usually called by :meth:`start`
        """
        # Acknowledgment has come
        # Send a READY for each available worker

        # Clear any workers if we SIGHUP'd
        for _ in self.workers:
            self.request_queue.put(None)
        self.workers = []

        for i in range(0, conf.WORKERS):
            self.send_ready()
            w = Worker(self.request_queue, self.finished_queue)
            w.start()
            self.workers.append(w)

        while True:
            if self.received_disconnect:
                for w in self.workers:
                    w.terminate()
                break

            events = self.poller.poll()

            if events.get(self.outgoing) == POLLIN:
                msg = self.outgoing.recv_multipart()
                self.process_message(msg)

            # Send readys for each finished job
            while True:
                try:
                    self.finished_queue.get_nowait()
                except Queue.Empty:
                    break
                else:
                    self.send_ready()

            if not self.maybe_send_heartbeat(events):
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

        # run callable
        payload = serializer(msg[2])
        # subcmd = payload[0]
        params = payload[1]

        self.request_queue.put(params)

    def send_ready(self):
        """
        send the READY command upstream to indicate that JobManager is ready
        for another REQUEST message.
        """
        sendmsg(self.outgoing, 'READY')

    def on_heartbeat(self, msgid, message):
        """
        a placeholder for a noop command. The actual 'logic' for HEARTBEAT is
        in :meth:`self.process_message` as every message is counted as a
        HEARTBEAT
        """

    def sighup_handler(self, signum, frame):
        logger.info('Caught signal %s' % signum)
        self.outgoing.rebuild()
        import_settings()
        self.start(addr=conf.WORKER_ADDR)

    def jobmanager_main(self):
        """
        Kick off jobmanager with logging and settings import
        """
        setup_logger('')
        import_settings()
        self.start(addr=conf.WORKER_ADDR)


def jobmanager_main():
    j = JobManager()
    j.jobmanager_main()
