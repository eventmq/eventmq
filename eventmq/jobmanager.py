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

from . import conf
from .poller import Poller, POLLIN
from .sender import Sender
from .utils.classes import EMQPService, HeartbeatMixin
from .utils.devices import generate_device_name
from .utils.messages import send_emqp_message as sendmsg
from .utils.timeutils import monotonic
from .worker import MultiprocessWorker as Worker

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
        logger.info('Initializing JobManager %s...'.format(self.name))

        #: Number of workers that are available to have a job executed. This
        #: number changes as workers become busy with jobs
        self.available_workers = 4

        #: JobManager starts out by INFORMing the router of it's existance,
        #: then telling the router that it is READY. The reply will be the unit
        #: of work.
        # Despite the name, jobs are received on this socket
        self.outgoing = Sender()

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

        while True:
            now = monotonic()
            events = self.poller.poll()

            if events.get(self.outgoing) == POLLIN:
                msg = self.outgoing.recv_multipart()
                self.process_message(msg)

            # Maintain the list of active jobs
            for job in self.active_jobs:
                if not job.is_alive():
                    self.active_jobs.remove(job)
                    self.available_workers += 1
                    self.send_ready()

            # TODO: Optimization: Move the method calls into another thread so
            # they don't block the event loop
            if not conf.DISABLE_HEARTBEATS:
                # Send a HEARTBEAT if necessary
                if now - self._meta['last_sent_heartbeat'] >= \
                   conf.HEARTBEAT_INTERVAL:
                    self.send_heartbeat(self.outgoing)

                # Do something about any missed HEARTBEAT, if we have nothing
                # waiting on the socket
                if self.is_dead() and not events:
                    logger.critical(
                        'The broker appears to have gone away. '
                        'Reconnecting...')
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

    def on_ack(self, msgid, ackd_msgid):
        """
        Sets :attr:`awaiting_ack` to False
        """
        # The msgid is the only frame in the message
        ackd_msgid = ackd_msgid[0]
        logger.info('Received ACK for router (or client) %s' % ackd_msgid)
        self.awaiting_startup_ack = False

    def on_heartbeat(self, msgid, message):
        """
        a placeholder for a noop command. The actual 'logic' for HEARTBEAT is
        in :meth:`self.process_message` as every message is counted as a
        HEARTBEAT
        """
