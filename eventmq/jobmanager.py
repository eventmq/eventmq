# This file is part of eventmq.
#
# eventmq is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# eventmq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with eventmq.  If not, see <http://www.gnu.org/licenses/>.
"""
:mod:`jobmanager` -- Job Manager
================================
Ensures things about jobs and spawns the actual tasks
"""
import json
import logging
import signal

from . import conf
from .poller import Poller, POLLIN
from .sender import Sender
from .utils.classes import EMQPService, HeartbeatMixin
from .utils.settings import import_settings
from .utils.devices import generate_device_name
from .utils.messages import send_emqp_message as sendmsg
from .utils.timeutils import monotonic
from .worker import MultiprocessWorker as Worker
from eventmq.log import setup_logger

logger = logging.getLogger(__name__)


class JobManager(HeartbeatMixin, EMQPService):
    """
    The exposed portion of the worker. The job manager's main responsibility is
    to manage the resources on the server it's running.

    This job manager uses tornado's eventloop.
    """
    SERVICE_TYPE = 'worker'

    def __init__(self, *args, **kwargs):
        """
        .. note ::
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

        #: Number of workers that are available to have a job executed. This
        #: number changes as workers become busy with jobs
        self.available_workers = 4

        #: JobManager starts out by INFORMing the router of it's existance,
        #: then telling the router that it is READY. The reply will be the unit
        #: of work.
        # Despite the name, jobs are received on this socket
        self.outgoing = Sender(name=self.name)

        #: Jobs that are running should be stored in `active_jobs`. There
        #: should always be at most `available_workers` count of active jobs.
        #: this point the manager should wait for a slot to free up.
        self.active_jobs = []

        self.poller = Poller()

        self._setup()

    def _start_event_loop(self):
        """
        Starts the actual eventloop. Usually called by :meth:`start`
        """
        # Acknowledgment has come
        # Send a READY for each available worker
        for i in range(0, self.available_workers):
            self.send_ready()


            # handle any sighups by reloading config
            signal.signal(signal.SIGHUP, self.sighup_handler)

        while True:
            if self.received_disconnect:
                # self.reset()
                # Shut down if there are no active jobs waiting
                if len(self.active_jobs) > 0:
                    self.prune_active_jobs()
                    continue
                break

            now = monotonic()
            events = self.poller.poll()

            if events.get(self.outgoing) == POLLIN:
                msg = self.outgoing.recv_multipart()
                self.process_message(msg)

            self.prune_active_jobs()

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
        payload = json.loads(msg[2])
        # subcmd = payload[0]
        params = payload[1]

        w = Worker()
        w.run(params)  # Spawns job w/ multiprocess
        self.active_jobs.append(w)

        self.available_workers -= 1

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

    def prune_active_jobs(self):
        # Maintain the list of active jobs
        for job in self.active_jobs:
            if not job.is_alive():
                self.active_jobs.remove(job)
                self.available_workers += 1

                if not self.received_disconnect:
                    self.send_ready()

    def sighup_handler(self, signum, frame):
        logger.info('Caught signal %s' % signum)
        self.incoming.unbind(conf.FRONTEND_ADDR)
        import_settings()
        self.start()

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


def jobmanager_main(self):
    j = JobManager()
    j.jobmanager_main()
