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
:mod:`scheduler` -- Scheduler
=============================
Handles cron and other scheduled tasks
"""
import json
import logging
import redis

from croniter import croniter
from six import next

from . import conf, constants
from .constants import KBYE
from .sender import Sender
from .poller import Poller, POLLIN
from .utils.classes import EMQPService, HeartbeatMixin
from json import loads as deserialize
from json import dumps as serialize
from .utils.messages import send_emqp_message as sendmsg
from .utils.settings import import_settings
from .utils.timeutils import IntervalIter
from .utils.timeutils import seconds_until, timestamp, monotonic
from .client.messages import send_request

from eventmq.log import setup_logger

logger = logging.getLogger(__name__)
CRON_CALLER_ID = -1
INFINITE_RUN_COUNT = -1


class Scheduler(HeartbeatMixin, EMQPService):
    """
    Keeper of time, master of schedules
    """
    SERVICE_TYPE = constants.CLIENT_TYPE.scheduler

    def __init__(self, *args, **kwargs):
        self.name = kwargs.get('name', None)

        logger.info('Initializing Scheduler...')
        import_settings()
        super(Scheduler, self).__init__(*args, **kwargs)
        self.outgoing = Sender()
        self._redis_server = None

        # contains dict of 4-item lists representing cron jobs
        # key of this dictionary is a hash of caller_id, path, and callable
        # from the message of the SCHEDULE command received
        # IDX     Description
        # 0 = the next ts this job should be executed in
        # 1 = the function to be executed
        # 2 = the croniter iterator for this job
        # 3 = the queue to execute the job in
        self.cron_jobs = {}

        # contains dict of 5-item lists representing jobs based on an interval
        # key of this dictionary is a hash of caller_id, path, and callable
        # from the message of the SCHEDULE command received
        # values of this list follow this format:
        # IDX     Descriptions
        # 0 = the next (monotonic) ts that this job should be executed in
        # 1 = the function to be executed
        # 2 = the interval iter for this job
        # 3 = the queue to execute the job in
        # 4 = run_count: # of times to execute this job
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
            callable_ = job[1].split('.')[-1]

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

            cron_hash = self.cron_hash(caller_id=CRON_CALLER_ID,
                                       path=path,
                                       callable_=callable_)

            self.cron_jobs[cron_hash] = [c_next, json.dumps(msg), c, None]

        try:
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
        except redis.ConnectionError:
            logger.warning('Could not contact redis server')
        except Exception as e:
            logger.warning(str(e))

    def _start_event_loop(self):
        """
        Starts the actual event loop. Usually called by :meth:`Scheduler.start`
        """
        while True:
            if self.received_disconnect:
                break

            ts_now = int(timestamp())
            m_now = monotonic()
            events = self.poller.poll()

            if events.get(self.outgoing) == POLLIN:
                msg = self.outgoing.recv_multipart()
                self.process_message(msg)

            # TODO: distribute me!
            for hash_, cron in self.cron_jobs.items():
                # If the time is now, or passed
                if cron[0] <= ts_now:
                    msg = cron[1]
                    queue = cron[3]

                    # Run the msg
                    logger.debug("Time is: %s; Schedule is: %s - Running %s"
                                 % (ts_now, cron[0], msg))

                    self.send_request(msg, queue=queue)

                    # Update the next time to run
                    cron[0] = next(cron[2])
                    logger.debug("Next execution will be in %ss" %
                                 seconds_until(cron[0]))

            cancel_jobs = []
            for k, v in self.interval_jobs.iteritems():
                # The schedule time has elapsed
                if v[0] <= m_now:
                    msg = v[1]
                    queue = v[3]

                    logger.debug("Time is: %s; Schedule is: %s - Running %s"
                                 % (ts_now, v[0], msg))

                    if v[4] != INFINITE_RUN_COUNT:
                        # Decrement run_count
                        v[4] -= 1
                        # If run_count was 0, we cancel the job
                        if v[4] <= 0:
                            cancel_jobs.append(k)
                        # Otherwise we run the job
                        else:
                            # Send job and update next schedule time
                            self.send_request(msg, queue=queue)
                            v[0] = next(v[2])
                            # Rename redis key and save new run_count counter
                            try:
                                self.redis_server.rename(k,
                                                         self.schedule_hash(v))
                                self.redis_server.set(self.schedule_hash(v),
                                                      serialize(v))
                                self.redis_server.save()
                            except redis.ConnectionError:
                                logger.warning("Couldn't contact redis server")
                            except Exception as e:
                                logger.warning(
                                    'Unable to update key in redis '
                                    'server: {}'.format(e.message))
                    else:
                        # Scheduled job is in running infinitely
                        # Send job and update next schedule time
                        self.send_request(msg, queue=queue)
                        v[0] = next(v[2])
                        # Persist changes to redis
                        try:
                            self.redis_server.set(
                                self.schedule_hash(v), serialize(v))
                            self.redis_server.save()
                        except redis.ConnectionError:
                            logger.warning("Couldn't contact redis server")
                        except Exception as e:
                            logger.warning(
                                'Unable to update key in redis '
                                'server: {}'.format(e.message))

            for job in cancel_jobs:
                message = self.interval_jobs[k][1]
                self.unschedule_job(message)
                del self.interval_jobs[k]

            if not self.maybe_send_heartbeat(events):
                break

    @property
    def redis_server(self):
        # Open connection to redis server for persistance
        if self._redis_server is None:
            try:
                self._redis_server = \
                    redis.StrictRedis(host=conf.RQ_HOST,
                                      port=conf.RQ_PORT,
                                      db=conf.RQ_DB,
                                      password=conf.RQ_PASSWORD)
                return self._redis_server

            except Exception as e:
                logger.warning('Unable to connect to redis server: {}'.format(
                    e.message))
        else:
            return self._redis_server

    def send_request(self, jobmsg, queue=None):
        """
        Send a request message to the broker

        Args:
            jobmsg: The message to send to the broker
            queue: The name of the queue to use_impersonation

        Returns:
            str: ID of the message
        """
        jobmsg = json.loads(jobmsg)
        msgid = send_request(self.outgoing, jobmsg, queue=queue)

        return msgid

    def on_disconnect(self, msgid, message):
        logger.info("Received DISCONNECT request: {}".format(message))
        self._redis_server.connection_pool.disconnect()
        sendmsg(self.outgoing, KBYE)
        self.outgoing.unbind(conf.SCHEDULER_ADDR)
        super(Scheduler, self).on_disconnect(msgid, message)

    def on_kbye(self, msgid, msg):
        if not self.is_heartbeat_enabled:
            self.reset()

    def on_unschedule(self, msgid, message):
        """
           Unschedule an existing schedule job, if it exists
        """
        logger.info("Received new UNSCHEDULE request: {}".format(message))

        # TODO: Notify router whether or not this succeeds
        self.unschedule_job(message)

    def unschedule_job(self, message):
        """
        Unschedules a job if it exists based on the message used to generate it
        """
        schedule_hash = self.schedule_hash(message)

        if schedule_hash in self.interval_jobs:
            # Remove scheduled job
            self.interval_jobs.pop(schedule_hash)
        elif schedule_hash in self.cron_jobs:
            # Remove scheduled job
            self.cron_jobs.pop(schedule_hash)
        else:
            logger.warning("Couldn't find matching schedule for unschedule " +
                           "request")

        # Double check the redis server even if we didn't find the hash
        # in memory
        try:
            if (self.redis_server.get(schedule_hash)):
                self.redis_server.lrem('interval_jobs', 0, schedule_hash)
                self.redis_server.save()
        except redis.ConnectionError:
            logger.warning('Could not contact redis server')
        except Exception as e:
            logger.warning(str(e))

    def load_job_from_redis(self, message):
        """
        """
        from .utils.timeutils import IntervalIter

        queue = message[0].encode('utf-8')
        headers = message[1]
        interval = int(message[2])
        inter_iter = IntervalIter(monotonic(), interval)
        schedule_hash = self.schedule_hash(message)
        cron = message[4] if interval == -1 else ""
        ts = int(timestamp())

        # Positive intervals are valid
        if interval >= 0:
            self.interval_jobs[schedule_hash] = [
                next(inter_iter),
                message[3],
                inter_iter,
                queue,
                self.get_run_count_from_headers(headers)
            ]
        # Non empty strings are valid
        # Expecting '* * * * *' etc.
        elif cron and cron != "":
            # Create the croniter iterator
            c = croniter(cron)

            # Get the next time this job should be run
            c_next = next(c)
            if ts >= c_next:
                # If the next execution time has passed move the iterator to
                # the following time
                c_next = next(c)

            self.cron_jobs[schedule_hash] = [c_next, message[3], c, queue]

    def on_schedule(self, msgid, message):
        """
        """
        logger.info("Received new SCHEDULE request: {}".format(message))

        queue = message[0]
        headers = message[1]
        interval = int(message[2])
        cron = str(message[4])

        schedule_hash = self.schedule_hash(message)

        # Notify if this is updating existing, or new
        if (schedule_hash in self.cron_jobs or
                schedule_hash in self.interval_jobs):
            logger.debug('Update existing scheduled job with %s'
                         % schedule_hash)
        else:
            logger.debug('Creating a new scheduled job with %s'
                         % schedule_hash)

        # If interval is negative, cron MUST be populated
        if interval >= 0:
            inter_iter = IntervalIter(monotonic(), interval)

            self.interval_jobs[schedule_hash] = [
                next(inter_iter),
                message[3],
                inter_iter,
                queue,
                self.get_run_count_from_headers(headers)
            ]

            if schedule_hash in self.cron_jobs:
                self.cron_jobs.pop(schedule_hash)
        else:
            ts = int(timestamp())
            c = croniter(cron)
            c_next = next(c)
            if ts >= c_next:
                # If the next execution time has passed move the iterator to
                # the following time
                c_next = next(c)

            self.cron_jobs[schedule_hash] = [c_next,
                                             message[3],
                                             c,
                                             None]

            if schedule_hash in self.interval_jobs:
                self.interval_jobs.pop(schedule_hash)

        # Persist the scheduled job
        try:
            if schedule_hash not in self.redis_server.lrange(
                    'interval_jobs', 0, -1):
                self.redis_server.lpush('interval_jobs', schedule_hash)
            self.redis_server.set(schedule_hash, serialize(message))
            self.redis_server.save()
        except redis.ConnectionError:
            logger.warning('Could not contact redis server')
        except Exception as e:
            logger.warning(str(e))

        if 'nohaste' not in headers:
            self.send_request(message[3], queue=queue)

    def get_run_count_from_headers(self, headers):
        run_count = INFINITE_RUN_COUNT
        for header in headers.split(','):
            if 'run_count:' in header:
                run_count = int(header.split(':')[1])
        return run_count

    def on_heartbeat(self, msgid, message):
        """
        Noop command. The logic for heartbeating is in the event loop.
        """

    def cron_hash(self, caller_id, path, callable_):
        schedule_hash_items = {'caller_id': caller_id,
                               'path': path,
                               'callable': callable_}

        # Hash the sorted, immutable set of items in our identifying dict
        schedule_hash = str(hash(tuple(frozenset(sorted(
            schedule_hash_items.items())))))

        return schedule_hash

    def schedule_hash(self, message):
        """
        Create a unique identifier for this message for storing
        and referencing later
        """
        # Items to use for uniquely identifying this scheduled job
        # TODO: Pass caller_id in a more rigid place
        msg = deserialize(message[3])[1]
        schedule_hash_items = {'caller_id': msg['class_args'][0],
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
    print("hello!")
