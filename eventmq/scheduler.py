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
from hashlib import sha1 as emq_hash
import json
from json import dumps as serialize
from json import loads as deserialize
import logging

from croniter import croniter
import redis
from six import next

from eventmq.log import setup_logger

from . import conf, constants
from .client.messages import send_request
from .constants import KBYE
from .poller import Poller, POLLIN
from .sender import Sender
from .utils.classes import EMQPService, HeartbeatMixin
from .utils.messages import send_emqp_message as sendmsg
from .utils.settings import import_settings
from .utils.timeutils import IntervalIter, monotonic, seconds_until, timestamp


logger = logging.getLogger(__name__)
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

        # contains dict of 4-item lists representing cron jobs key of this
        # dictionary is a hash of arguments, path, and callable from the
        # message of the SCHEDULE command received
        # IDX     Description
        # 0 = the next ts this job should be executed in
        # 1 = the function to be executed
        # 2 = the croniter iterator for this job
        # 3 = the queue to execute the job in
        self.cron_jobs = {}

        # contains dict of 5-item lists representing jobs based on an interval
        # key of this dictionary is a hash of arguments, path, and callable
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
        try:
            interval_job_list = self.redis_server.lrange(
                'interval_jobs', 0, -1)
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
                # TODO: Refactor this entire loop to be readable by humankind
                # The schedule time has elapsed
                if v[0] <= m_now:
                    msg = v[1]
                    queue = v[3]

                    logger.debug("Time is: %s; Schedule is: %s - Running %s"
                                 % (ts_now, v[0], msg))

                    # v[4] is the current remaining run_count
                    if v[4] != INFINITE_RUN_COUNT:
                        # If run_count was 0, we cancel the job
                        if v[4] <= 0:
                            cancel_jobs.append(k)
                        else:
                            # Decrement run_count
                            v[4] -= 1
                            # Persist the change to redis
                            try:
                                message = deserialize(self.redis_server.get(k))
                                new_headers = []
                                for header in message[1].split(','):
                                    if 'run_count:' in header:
                                        new_headers.append(
                                            'run_count:{}'.format(v[4]))
                                    else:
                                        new_headers.append(header)
                                message[1] = ",".join(new_headers)
                                self.redis_server.set(k, serialize(message))
                            except Exception as e:
                                logger.warning(
                                    'Unable to update key in redis '
                                    'server: {}'.format(e))
                            # Perform the request since run_count still > 0
                            self.send_request(msg, queue=queue)
                            v[0] = next(v[2])
                    else:
                        # Scheduled job is in running infinitely
                        # Send job and update next schedule time
                        self.send_request(msg, queue=queue)
                        v[0] = next(v[2])

            for job in cancel_jobs:
                try:
                    logger.debug('Cancelling job due to run_count: {}'
                                 .format(k))
                    self.redis_server.delete(k)
                    self.redis_server.lrem('interval_jobs', 0, k)
                except Exception as e:
                    logger.warning(
                        'Unable to update key in redis '
                        'server: {}'.format(e))
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
                    e))
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
        msgid = send_request(self.outgoing, jobmsg, queue=queue,
                             reply_requested=True)

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
                self.redis_server.delete(schedule_hash)
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
        run_count = self.get_run_count_from_headers(headers)

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
                run_count
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

            self.cron_jobs[schedule_hash] = [
                c_next, message[3], c, None]

            if schedule_hash in self.interval_jobs:
                self.interval_jobs.pop(schedule_hash)

        # Persist the scheduled job
        try:
            if schedule_hash not in self.redis_server.lrange(
                    'interval_jobs', 0, -1):
                self.redis_server.lpush('interval_jobs', schedule_hash)
            self.redis_server.set(schedule_hash, serialize(message))
            self.redis_server.save()
            logger.debug('Saved job {} with hash {} to redis'.format(
                message, schedule_hash))
        except redis.ConnectionError:
            logger.warning('Could not contact redis server. Unable to '
                           'guarantee persistence.')
        except Exception as e:
            logger.warning(str(e))

        # Send a request in haste mode, decrement run_count if needed
        if 'nohaste' not in headers:
            if run_count > 0 or run_count == INFINITE_RUN_COUNT:
                # Don't allow run_count to decrement below 0
                if run_count > 0:
                    self.interval_jobs[schedule_hash][4] -= 1
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

    @classmethod
    def schedule_hash(cls, message):
        """
        Create a unique identifier for this message for storing
        and referencing later

        Args:
            message (str): The serialized message passed to the scheduler

        Returns:
            int: unique hash for the job
        """

        # Get the job portion of the message
        msg = deserialize(message[3])[1]

        # Use json to create the hash string, sorting the keys.
        schedule_hash_items = json.dumps(
            {'args': msg['args'],
             'kwargs': msg['kwargs'],
             'class_args': msg['class_args'],
             'class_kwargs': msg['class_kwargs'],
             'path': msg['path'],
             'callable': msg['callable']},
            sort_keys=True)

        # Hash the sorted, immutable set of items in our identifying dict
        schedule_hash = emq_hash(
            schedule_hash_items.encode('utf-8')).hexdigest()

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


def test_job(*args, **kwargs):
    """
    Simple test job for use with the scheduler
    """
    print("hello!")  # noqa
