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
:mod:`scheduler` -- Scheduler
=============================
Handles cron and other scheduled tasks
"""
import logging
import time

from croniter import croniter
from six import next

from . import conf
from .sender import Sender
from .utils.classes import HeartbeatMixin
from .utils.settings import import_settings
from .utils.timeutils import seconds_until, timestamp
from .client.messages import send_request

from eventmq.log import setup_logger

logger = logging.getLogger(__name__)


class Scheduler(HeartbeatMixin):
    """
    Keeper of time, master of schedules
    """

    def __init__(self, *args, **kwargs):
        logger.info('Initializing Scheduler...')
        super(Scheduler, self).__init__(*args, **kwargs)
        self.outgoing = Sender()

        # 0 = the next ts this job should be executed
        # 1 = the function to be executed
        # 2 = the croniter iterator for this job
        self.jobs = []

        self.load_jobs()

    def connect(self, addr='tcp://127.0.0.1:47290'):
        """
        Connect the scheduler to worker/router at `addr`
        """
        self.outgoing.connect(addr)

    def load_jobs(self):
        """
        Loads the jobs that need to be scheduled
        """
        raw_jobs = (
            ('* * * * *', 'eventmq.scheduler.test_job'),
        )
        ts = int(timestamp())
        for job in raw_jobs:
            # Create the croniter iterator
            c = croniter(job[0])

            # Get the next time this job should be run
            c_next = next(c)
            if ts >= c_next:
                # If the next execution time has passed move the iterator to
                # the following time
                c_next = next(c)
            self.jobs.append([c_next, job[1], c])

    def start(self, addr='tcp://127.0.0.1:47290'):
        """
        Begin sending messages to execute scheduled jobs
        """
        self.connect(addr)

        self._start_event_loop()

    def _start_event_loop(self):
        """
        Starts the actual event loop. Usually called by :meth:`Scheduler.start`
        """
        while True:
            ts_now = int(timestamp())

            for i in range(0, len(self.jobs)):
                if self.jobs[i][0] <= ts_now:  # If the time is now, or passed
                    job = self.jobs[i][1]
                    path = '.'.join(job.split('.')[:-1])
                    callable_ = job.split('.')[-1]

                    # Run the job
                    logger.debug("Time is: %s; Schedule is: %s - Running %s"
                                 % (ts_now, self.jobs[i][0], job))

                    msg = ['run', {
                        'path': path,
                        'callable': callable_
                    }]
                    send_request(self.outgoing, msg)

                    # Update the next time to run
                    self.jobs[i][0] = next(self.jobs[i][2])
                    logger.debug("Next execution will be in %ss" %
                                 seconds_until(self.jobs[i][0]))

            time.sleep(0.1)

    def scheduler_main(self):
        """
        Kick off scheduler with logging and settings import
        """
        setup_logger("eventmq")
        import_settings()
        self.start(addr=conf.SCHEDULER_ADDR)


# Entry point for pip console scripts
def scheduler_main():
    s = Scheduler()
    s.scheduler_main()


def test_job():
    print "hello!"
    print "hello!"
    print "hello!"
    print "hello!"
    time.sleep(4)
