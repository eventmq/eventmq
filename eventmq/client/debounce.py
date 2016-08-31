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
import logging
import copy

from .. import conf

logger = logging.getLogger(__name__)

# attempt to import redis
try:
    import redis  # noqa
except ImportError:
    redis = None
    logger.warning('Redis not installed, debounce support not available.')


def _debounce_run_deferred_job(context):
    """Called by the debounce system to actually run the job.

    This is called by the debounce system at the end of the process, when the
    job should actually run.  It deserializes the callable, the arguments, and
    tries to execute the callable with the arguments.

    context keys:
        path (str): The path of the callable function (the module information)
        callable_name (str): The name of the callable
        args (list): The arguments to pass when invoking the callable
        kwargs (dict): The arguments to pass when invoking the callable
        class_args (list): Arguments to use when initializing the callable
            class (if it is a class)
        class_kwargs (dict): The arguments to use when initializing the
            callable class (if it is a class)
        cache_key1 (str): The cache key for step 1
        cache_key2 (str): The cache key for step 2
        queue (str): The queue name for this job
        job_id (str): The unique job identifier
        scheduled (bool): Whether or not this was deferred immediately, or
            scheduled for the future.

    Args:
       context (dict): See above for "context keys"
    """
    from ..sender import Sender
    from ..utils.functions import run_function
    from .messages import schedule

    context = copy.deepcopy(context)  # don't modify caller's context
    queue = context.get('queue')
    job_id = context.get('job_id')
    path = context.get('path')
    callable_name = context.get('callable_name')
    scheduled = context.get('scheduled')
    cache_key1 = context.get('cache_key1')
    cache_key2 = context.get('cache_key2')
    args = context.get('args')
    kwargs = context.get('kwargs')
    class_args = context.get('class_args')
    class_kwargs = context.get('class_kwargs')

    if queue:
        queue = str(queue)

    if job_id:
        job_id = str(job_id)

    logger.debug('DEBOUNCE: {}.{} - running {} job'.format(
        path, callable_name, 'scheduled' if job_id else 'deferred'))

    if scheduled and job_id:
        # TODO: here we create a new socket just so we can unschedule the
        # debunce scheduled job.  This can be removed once the `run_x` feature
        # is implemented that lets you specify how many times a scheduled job
        # should run before stopping.
        socket = Sender()
        socket.connect(addr=conf.FRONTEND_ADDR)
        schedule(socket, _debounce_run_deferred_job,
                 queue=queue, unschedule=True, class_args=(job_id, ))
        socket.zsocket.close()

        del socket

    try:
        redis_connection = redis.StrictRedis(
            host=conf.RQ_HOST,
            port=conf.RQ_PORT,
            db=conf.RQ_DB,
            password=conf.RQ_PASSWORD)
    except Exception as e:
        logger.error('DEBOUNCE: {}.{} - requested, but could not connect '
                     'to redis server'.format(path, callable_name, e))
        redis_connection.delete(cache_key1)
        if scheduled:
            redis_connection.delete(cache_key2)
        return

    # if the timer countdown caused this function to run, and `cache_key1` is
    # set, bail out.  Otherwise, continue, because the cache is set prior to
    # deferring the function.
    if redis_connection.get(cache_key1) and scheduled:
        logger.debug('DEBOUNCE: {}.{} - cache_key1 was set, not '
                     'running job.'.format(path, callable_name))
        return

    redis_connection.set(cache_key1, 1)
    redis_connection.expire(
        cache_key1, getattr(conf, 'DEBOUNCE_CACHE_KEY1_TIMEOUT', 60))

    try:
        run_function(
            path, callable_name, class_args, class_kwargs, *args, **kwargs)
    except Exception as e:
        logger.error('DEBOUNCE: {}.{} - exception {}'.format(
            path, callable_name, e))

    redis_connection.delete(cache_key1)
    if scheduled:
        redis_connection.delete(cache_key2)


def _debounce_deferred_job(context):
    """Debounce the job.

    context keys:
        path (str): The path of the callable function (the module information)
        callable_name (str): The name of the callable
        args (list): The arguments to pass when invoking the callable
        kwargs (dict): The arguments to pass when invoking the callable
        class_args (list): Arguments to use when initializing the callable
            class (if it is a class)
        class_kwargs (dict): The arguments to use when initializing the
            callable class (if it is a class)
        queue (str): The queue name for this job
        debounce_secs (int): Debounce schedule interval, in seconds

    Args:
        context (dict): See "context keys" above.
    """
    from ..sender import Sender
    from ..utils.functions import arguments_hash
    from .messages import schedule

    job_id = arguments_hash(locals())
    context = copy.deepcopy(context)  # don't modify caller's context
    path = context.get('path')
    callable_name = context.get('callable_name')
    debounce_secs = context.pop('debounce_secs')

    if not redis:
        logger.error('DEBOUNCE requested, but redis is not available.')
        return

    try:
        redis_connection = redis.StrictRedis(
            host=conf.RQ_HOST,
            port=conf.RQ_PORT,
            db=conf.RQ_DB,
            password=conf.RQ_PASSWORD)
    except Exception as e:
        logger.error('Debounce requested, but could not connect to '
                     'redis server: {}.'.format(e))
        return

    cache_key1 = '{}_key1'.format(job_id)
    cache_key2 = '{}_key2'.format(job_id)

    logger.debug('DEBOUNCE: {}.{} - job id: {}'.format(
        path, callable_name, job_id))

    context.update({
        'cache_key1': cache_key1,
        'cache_key2': cache_key2,
        'scheduled': False,
        })

    if not redis_connection.get(cache_key1):
        logger.debug('DEBOUNCE: {}.{} - cache_key1 was not set, '
                     'deferring immediately.'.format(path, callable_name))
        redis_connection.set(cache_key1, 1)
        redis_connection.expire(
            cache_key1, getattr(conf, 'DEBOUNCE_CACHE_KEY1_TIMEOUT', 60))

        _debounce_run_deferred_job(context)
        return
    else:
        logger.debug(
            'DEBOUNCE: {}.{} - cache_key1 was set, '
            'trying cache_key2...'.format(path, callable_name))

    if not redis_connection.get(cache_key2):
        logger.debug('DEBOUNCE: {}.{} - cache_key2 was not set, scheduling '
                     'for {} seconds'.format(path, callable_name,
                                             debounce_secs))
        redis_connection.set(cache_key2, 1)
        redis_connection.expire(cache_key2, debounce_secs)

        context['job_id'] = job_id
        context['scheduled'] = True

        # TODO: here we create a new socket just so we can schedule the
        # debunce scheduled job.  This should probably use the socket that was
        # passed to the original `defer_job` call, however, this is not
        # currently possible in eventmq.
        socket = Sender()
        socket.connect(addr=conf.FRONTEND_ADDR)

        schedule(
            socket=socket,
            func=_debounce_run_deferred_job,
            queue=context.get('queue'),
            interval_secs=debounce_secs,
            kwargs={'context': context},
            headers=('nohaste', 'guarantee'),
            class_args=(job_id, ),
        )
        socket.zsocket.close()

        del socket
        return
    else:
        logger.debug(
            'DEBOUNCE: {}.{} - cache_key2 was set, dropping message.  '.format(
                path, callable_name))


def _debounce_schedule(
        socket, path, callable_name, debounce_secs, args=(), kwargs=None,
        class_args=(), class_kwargs=None, reply_requested=False,
        guarantee=False, retry_count=0, queue=conf.DEFAULT_QUEUE_NAME):
    """Schedule the initial debounce.

    Debounce works like this:

    1.  When the first job comes in, `cache_key1` will not be set, so it
    will defer the job to run immediately.  At the beginning of that job,
    it will set `cache_key1`, and at the end, it will unset `cache_key1`.

    2.  If `cache_key1` is set and a new job comes in, if `cache_key2` is
    not set, it will set `cache_key2` and schedule the job to run
    `debounce_secs` in the future.

    3.  If a third job comes in while `cache_key1` and `cache_key2` are
    set, it will be ignored.

    Args:
        path (str): The path of the callable function (the module information)
        callable_name (str): The name of the callable
        debounce_secs (int): Debounce schedule interval, in seconds
        reply_requested (bool): request the return value of func as a reply
        retry_count (int): How many times should be retried when encountering
            an Exception or some other failure before giving up. (default: 0
            or immediately fail)
        queue (str): Name of queue to use when executing the job. If this value
            evaluates to False, the default is used. Default: is configured
            default queue name
    """

    from .messages import defer_job

    logger.debug('DEBOUNCE: {}.{} - initial schedule'.format(
        path, callable_name))

    context = {
        'path': path,
        'callable_name': callable_name,
        'args': args,
        'kwargs': kwargs,
        'class_args': class_args,
        'class_kwargs': class_kwargs,
        'queue': queue,
        'scheduled': False,
        'debounce_secs': debounce_secs,
    }

    defer_job(
        socket,
        _debounce_deferred_job,
        reply_requested=reply_requested,
        retry_count=retry_count,
        queue=queue,
        guarantee=guarantee,
        kwargs={'context': context},
        )
