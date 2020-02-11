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
from __future__ import print_function

from hashlib import sha1 as emq_hash
import importlib
import json
from json import dumps as serialize
from json import loads as deserialize
import logging
import sys

from croniter import croniter
from six import iteritems, next

from eventmq.log import setup_logger
from . import __version__
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

        logger.info('EventMQ Version {}'.format(__version__))
        logger.info('Initializing Scheduler...')
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

        self._setup()

    def load_jobs(self):
        """
        Loads the jobs from redis that need to be scheduled
        """
        if self.redis_server:
            try:
                interval_job_list = self.redis_server.lrange(
                    'interval_jobs', 0, -1)
                if interval_job_list is not None:
                    for i in interval_job_list:
                        logger.debug('Restoring job with hash %s' % i)
                        if self.redis_server.get(i):
                            self.load_job_from_redis(
                                message=deserialize(self.redis_server.get(i)))
                        else:
                            logger.warning(
                                'Expected scheduled job in redis, but none '
                                'was found with hash {}'.format(i))
            except Exception as e:
                logger.warning(str(e), exc_info=True)

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
            for k, v in iteritems(self.interval_jobs):
                if v[0] <= m_now:
                    # The schedule time has elapsed
                    msg = v[1]
                    queue = v[3]

                    logger.debug("Time is: {}; Schedule is: {} - Running {} "
                                 "({})".format(m_now, v[0], k, msg))

                    # v[4] is the current remaining run_count
                    if v[4] != INFINITE_RUN_COUNT:
                        # If run_count was 0, we cancel the job
                        if v[4] <= 0:
                            cancel_jobs.append(k)
                        else:
                            # Decrement run_count
                            v[4] -= 1
                            if v[4] > 0:
                                # Update the next run time
                                v[0] = next(v[2])
                                # Persist the change to redis if there are run
                                # counts still left
                                if self.redis_server:
                                    try:
                                        message = deserialize(
                                            self.redis_server.get(k))
                                        new_headers = []
                                        for header in message[1].split(','):
                                            if 'run_count:' in header:
                                                new_headers.append(
                                                    'run_count:{}'.format(
                                                        v[4]))
                                            else:
                                                new_headers.append(header)
                                        message[1] = ",".join(new_headers)
                                        self.redis_server.set(
                                            k, serialize(message))
                                    except Exception as e:
                                        logger.warning(
                                            'Unable to update key in redis '
                                            'server: {}'.format(e))
                            else:
                                cancel_jobs.append(k)

                            # Perform the job since run_count was still > 0
                            self.send_request(msg, queue=queue)
                    else:
                        # Scheduled job is in running infinitely
                        # Send job and update next schedule time
                        self.send_request(msg, queue=queue)
                        v[0] = next(v[2])

            for job_id in cancel_jobs:
                self.cancel_job(job_id)

            if not self.maybe_send_heartbeat(events):
                break

    @property
    def redis_server(self):
        if self._redis_server is None:
            # Open connection to redis server for persistence
            cls_split = conf.REDIS_CLIENT_CLASS.split('.')
            cls_path, cls_name = '.'.join(cls_split[:-1]), cls_split[-1]
            try:
                mod = importlib.import_module(cls_path)
                redis_cls = getattr(mod, cls_name)
            except (ImportError, AttributeError) as e:
                errmsg = 'Unable to import redis_client_class {} ({})'.format(
                    conf.REDIS_CLIENT_CLASS, e)
                logger.warning(errmsg)

                if conf.REDIS_STARTUP_ERROR_HARD_KILL:
                    sys.exit(1)
                return None

            url = 'redis://{}:{}/{}'.format(
                conf.RQ_HOST, conf.RQ_PORT, conf.RQ_DB)

            logger.info('Connecting to redis: {}{}'.format(
                url, ' with password' if conf.RQ_PASSWORD else ''))

            if conf.RQ_PASSWORD:
                url = 'redis://:{}@{}:{}/{}'.format(
                    conf.RQ_PASSWORD, conf.RQ_HOST, conf.RQ_PORT,
                    conf.RQ_DB)

            try:
                self._redis_server = \
                    redis_cls.from_url(url, **conf.REDIS_CLIENT_CLASS_KWARGS)
            except Exception as e:
                logger.warning('Unable to connect to redis server: {}'.format(
                    e))
                if conf.REDIS_STARTUP_ERROR_HARD_KILL:
                    sys.exit(1)

            return None

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
        """Process request to shut down."""
        logger.info("Received DISCONNECT request: {}".format(message))
        sendmsg(self.outgoing, KBYE)
        self.outgoing.unbind(conf.SCHEDULER_ADDR)

        if self._redis_server:
            # Check the internal var. No need to connect if we haven't already
            # connected by this point
            try:
                self._redis_server.connection_pool.disconnect()
            except Exception:
                pass
        super(Scheduler, self).on_disconnect(msgid, message)

    def on_kbye(self, msgid, msg):
        """Process router going offline"""
        if not self.is_heartbeat_enabled:
            self.reset()

    def on_unschedule(self, msgid, message):
        """Unschedule an existing schedule job, if it exists."""
        logger.debug("Received new UNSCHEDULE request: {}".format(message))

        schedule_hash = self.schedule_hash(message)
        # TODO: Notify router whether or not this succeeds
        self.cancel_job(schedule_hash)

    def cancel_job(self, schedule_hash):
        """
        Cancels a job if it exists

        Args:
            schedule_hash (str): The schedule's unique hash.
                See :meth:`Scheduler.schedule_hash`
        """
        if schedule_hash in self.interval_jobs:
            # If the hash wasn't found in either `cron_jobs` or `interval_jobs`
            # then it's safe to assume it's already deleted.
            try:
                del self.interval_jobs[schedule_hash]
            except KeyError:
                pass
            try:
                del self.cron_jobs[schedule_hash]
            except KeyError:
                pass

        # Double check the redis server even if we didn't find the hash
        # in memory
        try:
            if self.redis_server:
                self.redis_server.lrem('interval_jobs', 0, schedule_hash)
        except Exception as e:
            logger.warning(str(e), exc_info=True)
        try:
            if self.redis_server and self.redis_server.get(schedule_hash):
                self.redis_server.delete(schedule_hash)
        except Exception as e:
            logger.warning(str(e), exc_info=True)

    def load_job_from_redis(self, message):
        """Parses and loads a message from redis as a scheduler job."""
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
        """Create a new scheduled job."""
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
        if self.redis_server:
            try:
                if schedule_hash not in self.redis_server.lrange(
                        'interval_jobs', 0, -1):
                    self.redis_server.lpush('interval_jobs', schedule_hash)
                    self.redis_server.set(schedule_hash, serialize(message))
                logger.debug('Saved job {} with hash {} to redis'.format(
                    message, schedule_hash))
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
        """Noop command. The logic for heart beating is in the event loop."""

    @classmethod
    def schedule_hash(cls, message):
        """Create a unique identifier to store and reference a message later.

        Args:
            message (str): The serialized message passed to the scheduler

        Returns:
            str: unique hash for the job
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
        import_settings('scheduler')

        self.load_jobs()
        self.start(addr=conf.SCHEDULER_ADDR)


def test_job(*args, **kwargs):
    """
    Simple test job for use with the scheduler
    """
    from pprint import pprint
    print("hello!")  # noqa
    pprint(args)  # noqa
    pprint(kwargs)  # noqa
