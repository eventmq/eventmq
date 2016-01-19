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
from json import loads as deserialize
from json import dumps as serialize
from .utils.settings import import_settings
from .utils.timeutils import IntervalIter
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
                                              db=0)

        # contains 4-item lists representing cron jobs
        # IDX     Description
        # 0 = the next ts this job should be executed in
        # 1 = the function to be executed
        # 2 = the croniter iterator for this job
        # 3 = the queue to execute the job in
        self.cron_jobs = []

        # contains dict of 4-item lists representing jobs based on an interval
        # key of this dictionary is a hash of company_id, path, and callable
        # from the message of the SCHEDULE command received
        # values of this list follow this format:
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
            interval_job_list = self.redis_server.lrange('interval_jobs',
                                                         0,
                                                         -1)
            if interval_job_list is not None:
                for i in interval_job_list:
                    logger.debug('Restoring job with hash %s' % i)
                    if (self.redis_server.get(i)):
                        self.load_job_from_redis(
                            message=deserialize(self.redis_server.get(i)))
                    else:
                        logger.warning('Expected scheduled job in redis,' +
                                       'but none was found with hash %s' % i)
            else:
                logger.warning('Unabled to talk to redis server')

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

            for k, v in self.interval_jobs.iteritems():
                if v[0] <= m_now:
                    msg = v[1]
                    queue = v[3]

                    logger.debug("Time is: %s; Schedule is: %s - Running %s"
                                 % (ts_now, v[0], msg))

                    self.send_request(msg, queue=queue)
                    v[0] = next(v[2])

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

        schedule_hash = self.schedule_hash(message)

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
                self.redis_server.lpop(schedule_hash)
                self.redis_server.set('interval_jobs',
                                      self.interval_jobs.keys())
                self.redis_server.save()

    def load_job_from_redis(self, message):
        """
        """
        from .utils.timeutils import IntervalIter

        queue = message[0].encode('utf-8')
        interval = int(message[2])
        inter_iter = IntervalIter(monotonic(), interval)
        schedule_hash = self.schedule_hash(message)

        self.interval_jobs[schedule_hash] = [
            next(inter_iter),
            message[3],
            inter_iter,
            queue
        ]

    def on_schedule(self, msgid, message):
        """
        """
        logger.info("Received new SCHEDULE request: {}".format(message))

        queue = message[0]
        interval = int(message[2])
        inter_iter = IntervalIter(monotonic(), interval)
        schedule_hash = self.schedule_hash(message)

        # Notify if this is updating existing, or new
        if (schedule_hash in self.interval_jobs):
            logger.debug('Update existing scheduled job with %s'
                         % schedule_hash)
        else:
            logger.debug('Creating a new scheduled job with %s'
                         % schedule_hash)

        self.interval_jobs[schedule_hash] = [
            next(inter_iter),
            message[3],
            inter_iter,
            queue
        ]

        # Persist the scheduled job
        if (self.redis_server):
            if schedule_hash not in self.redis_server.lrange('interval_jobs',
                                                             0,
                                                             -1):
                self.redis_server.lpush('interval_jobs', schedule_hash)
            self.redis_server.set(schedule_hash, serialize(message))
            self.redis_server.save()

        self.send_request(message[3], queue=queue)

    def on_heartbeat(self, msgid, message):
        """
        Noop command. The logic for heartbeating is in the event loop.
        """

    def schedule_hash(self, message):
        """
        Create a unique identifier for this message for storing
        and referencing later
        """
        # Items to use for uniquely identifying this scheduled job
        # TODO: Pass company_id in a more rigid place
        msg = deserialize(message[3])[1]
        schedule_hash_items = {'company_id': msg['class_args'][0],
                               'path': msg['path'],
                               'callable': msg['callable']}

        # Hash the sorted, immutable set of items in our identifying dict
        schedule_hash = str(hash(tuple(frozenset(sorted(
            schedule_hash_items.items())))))

        return schedule_hash

    def scheduler_main(self):
        """
        Kick off scheduler with logging and settings import
        """
        setup_logger("eventmq")
        import_settings()
        self.__init__()
        self.start(addr=conf.SCHEDULER_ADDR)


# Entry point for pip console scripts
def scheduler_main():
    s = Scheduler()
    s.scheduler_main()


def test_job():
    print "hello!"
    print "hello!"
    time.sleep(4)
