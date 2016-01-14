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
import json
import logging
import time
import redis

from croniter import croniter
from six import next

from . import conf
from .sender import Sender
from .poller import Poller, POLLIN
from .utils.classes import EMQPService, HeartbeatMixin
from .utils.settings import import_settings
from .utils.timeutils import seconds_until, timestamp, monotonic
from .client.messages import send_request

from eventmq.log import setup_logger

logger = logging.getLogger(__name__)


class Scheduler(HeartbeatMixin, EMQPService):
    """
    Keeper of time, master of schedules
    """
    SERVICE_TYPE = 'scheduler'

    def __init__(self, *args, **kwargs):
        logger.info('Initializing Scheduler...')
        super(Scheduler, self).__init__(*args, **kwargs)
        self.outgoing = Sender()

        # Open connection to redis server for persistance
        self.redis_server = redis.StrictRedis(host='localhost',
                                              port=6379,
                                              db=0,
                                              max_connections=1)

        # contains 4-item lists representing cron jobs
        # IDX     Description
        # 0 = the next ts this job should be executed in
        # 1 = the function to be executed
        # 2 = the croniter iterator for this job
        # 3 = the queue to execute the job in
        self.cron_jobs = []

        # contains dict of 4-item lists representing jobs based on an interval
        # IDX     Descriptions
        # 0 = the next (monotonic) ts that this job should be executed in
        # 1 = the function to be executed
        # 2 = the interval iter for this job
        # 3 = the queue to execute the job in
        self.interval_jobs = {}

        self.poller = Poller()

        self.load_jobs()

        self._setup()

    def load_jobs(self):
        """
        Loads the jobs that need to be scheduled
        """
        raw_jobs = (
            # ('* * * * *', 'eventmq.scheduler.test_job'),
        )
        ts = int(timestamp())
        for job in raw_jobs:
            # Create the croniter iterator
            c = croniter(job[0])
            path = '.'.join(job[1].split('.')[:-1])
            callable_ = job.split('.')[-1]

            msg = ['run', {
                'path': path,
                'callable': callable_
            }]

            # Get the next time this job should be run
            c_next = next(c)
            if ts >= c_next:
                # If the next execution time has passed move the iterator to
                # the following time
                c_next = next(c)
            self.cron_jobs.append([c_next, msg, c, None])

        # Restore persisted data if redis connection is alive and has jobs
        if (self.redis_server):
            interval_job_list = self.redis_server.get('interval_jobs')
            for i in interval_job_list:
                if (self.redis_server.get(i)):
                    self.interval_jobs.append(i, self.redis_server.get(i))
                else:
                    logger.warning('Expected scheduled job in redis server,' +
                                   'but none was found')

    def _start_event_loop(self):
        """
        Starts the actual event loop. Usually called by :meth:`Scheduler.start`
        """
        while True:
            ts_now = int(timestamp())
            m_now = monotonic()
            events = self.poller.poll()

            if events.get(self.outgoing) == POLLIN:
                msg = self.outgoing.recv_multipart()
                self.process_message(msg)

            # TODO: distribute me!
            for i in range(0, len(self.cron_jobs)):
                # If the time is now, or passed
                if self.cron_jobs[i][0] <= ts_now:
                    msg = self.cron_jobs[i][1]
                    queue = self.cron_jobs[i][3]

                    # Run the msg
                    logger.debug("Time is: %s; Schedule is: %s - Running %s"
                                 % (ts_now, self.cron_jobs[i][0], msg))

                    self.send_request(self.outgoing, msg, queue=queue)

                    # Update the next time to run
                    self.cron_jobs[i][0] = next(self.cron_jobs[i][2])
                    logger.debug("Next execution will be in %ss" %
                                 seconds_until(self.cron_jobs[i][0]))

            for k, v in self.interval_jobs:
                if v[0] <= m_now:
                    msg = v[1]
                    queue = v[i][3]

                    logger.debug("Time is: %s; Schedule is: %s - Running %s"
                                 % (ts_now, v[i][0], msg))

                    self.send_request(msg, queue=queue)
                    v[i][0] = next(v[i][2])

            if not self.maybe_send_heartbeat(events):
                break

    def send_request(self, jobmsg, queue=None):
        jobmsg = json.loads(jobmsg)
        send_request(self.outgoing, jobmsg, queue=queue)

    def on_unschedule(self, msgid, message):
        """
           Unschedule an existing schedule job, if it exists
        """
        logger.info("Received new UNSCHEDULE request: {}".format(message))

        # Items to use for uniquely identifying this scheduled job
        # TODO: Pass company_id in a more rigid place
        schedule_hash_items = {'company_id': message[2]['args'][0],
                               'path': message[2]['path'],
                               'callable': message[2]['callable']}

        # Hash the sorted, immutable set of items in our identifying dict
        schedule_hash = str(hash(tuple(frozenset(sorted(
            schedule_hash_items.items())))))

        if schedule_hash in self.interval_jobs:
            # Remove scheduled job
            self.interval_jobs.pop(schedule_hash)

        else:
            logger.debug("Couldn't find matching schedule for unschedule " +
                         "request")

        # Double check the redis server even if we didn't find the hash
        # in memory
        if (self.redis_server):
            if (self.redis_server.get(schedule_hash)):
                self.redis_server.delete(schedule_hash)
                self.redis_server.set('interval_jobs', self.interval_jobs)
                self.redis_server.save()

    def on_schedule(self, msgid, message):
        """
        """
        from .utils.timeutils import IntervalIter

        logger.info("Received new SCHEDULE request: {}".format(message))

        queue = message[0]
        interval = int(message[1])
        inter_iter = IntervalIter(monotonic(), interval)

        # Items to use for uniquely identifying this scheduled job
        # TODO: Pass company_id in a more rigid place
        schedule_hash_items = {'company_id': message[2]['args'][0],
                               'path': message[2]['path'],
                               'callable': message[2]['callable']}

        # Hash the sorted, immutable set of items in our identifying dict
        schedule_hash = str(hash(tuple(frozenset(sorted(
            schedule_hash_items.items())))))

        # Notify if this is updating existing, or new
        if (schedule_hash in self.interval_jobs):
            logger.debug('Update existing scheduled job with %s'
                         % schedule_hash)
        else:
            logger.debug('Creating a new scheduled job with %s'
                         % schedule_hash)

        self.interval_jobs[schedule_hash] = [
            next(inter_iter),
            message[2],
            inter_iter,
            queue
        ]

        # Persist the scheduled job
        if (self.redis_server):
            self.redis_server.set(schedule_hash, message[2])
            self.redis_server.set('interval_jobs', self.interval_jobs.keys())
            self.redis_server.save()

        self.send_request(message[2], queue=queue)

    def on_heartbeat(self, msgid, message):
        """
        Noop command. The logic for heartbeating is in the event loop.
        """

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
    time.sleep(4)
