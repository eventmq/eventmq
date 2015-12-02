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
import time

from croniter import croniter
from six import next

from . import log
from .sender import Sender
from .utils.classes import HeartbeatMixin
from .utils.devices import generate_device_name
from .utils.timeutils import monotonic, seconds_until, timestamp

logger = log.get_logger(__file__)


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

    def connect(self, addr=''):
        """
        Connect the scheduler to worker/router at `addr`
        """

    def load_jobs(self):
        """
        Loads the jobs that need to be scheduled
        """
        raw_jobs = (
            # heartbeat emails
            ('* * * * *', 'health.cron.check_device_health'),  # renew winrt push tokens
            ('30 12,21 * * *', 'apns.cron.winrt_refresh_credential'),  # renew calendar watches
            ('0 11 * * *', 'calendars.cron.renew_watches'),
            ('0 * * * *', 'calendars.cron.send_checkin_notifications'),
            ('* * * * *', 'calendars.cron.mark_events_as_ended'),
            ('* * * * *', 'calendars.cron.exchange2007_polling'),  # renew calendar watches (exchange)
            ('0 */2 * * *', 'calendars.cron.renew_ews_push_tokens'),  # renew calendar watches (exchange)
            ('1 5 * * *', 'calendars.cron.delete_old_events'),
            ('0 8 * * *', 'support.cron.expire_stale_support_pins'),
            ('5 8 * * 6', 'analytics.cron.weekly_mozilla_report'),
            ('0 10 * * *', 'crm.cron.unmark_out_of_office'),
            ('1 6 * * *', 'devices.cron.status_cleanup'),
            ('42 13 * * *', 'drip.cron.send_drips'),
            ('0 9 * * *', 'billing.cron.suspend_expired_trials'),
            ('1 6 * * *', 'billing.cron.create_subscription_invoices'),  # run at 8AM EST
            # ('0 12 * * *',           'analytics.cron.hunt_zombies'),
            ('0 * * * *',
             'integrations.plugins.o365.tasks.sync_all_active_plugins')
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
            m_now = monotonic()

            for i in range(0, len(self.jobs)):
                if self.jobs[i][0] <= ts_now:  # If the time is now, or passed
                    # Run the job
                    logger.debug("Time is: %s; Schedule is: %s - Running %s"
                                 % (ts_now, self.jobs[i][0], self.jobs[i][1]))
                    # Update the next time to run
                    self.jobs[i][0] = next(self.jobs[i][2])
                    logger.debug("Next execution will be in %ss" %
                                 seconds_until(self.jobs[i][0]))

            time.sleep(1)
