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
:mod:`messages` -- Client Messaging
===================================
"""
from json import dumps as serialize
import logging

from past.builtins import basestring

from .. import conf
from ..utils.functions import name_from_callable, split_callable_name
from ..utils.messages import send_emqp_message

logger = logging.getLogger(__name__)


def schedule(socket, func, interval_secs=None, args=(), kwargs=None,
             class_args=(), class_kwargs=None, headers=('guarantee',),
             queue=conf.DEFAULT_QUEUE_NAME, unschedule=False, cron=None):
    """
    Execute a task on a defined interval.

    .. note::

       All passed class & fuction kwargs/args MUST be json serializable.

    Args:
        socket (socket): eventmq socket to use for sending the message
        func (callable): the callable (or string path to calable) to be
            scheduled on a worker
        interval_secs (int): Run job every interval_secs or None if using cron
        args (list): list of *args to pass to the callable
        cron (string): cron formatted string used for job schedule if
            interval_secs is None, i.e. '* * * * *' (every minute)
        kwargs (dict): dict of **kwargs to pass to the callable
        class_args (list): list of *args to pass to the class (if applicable)
        class_kwargs (dict): dict of **kwargs to pass to the class (if
            applicable)
        headers (list): list of strings denoting enabled headers. Default:
            guarantee is enabled to ensure the scheduler schedules the job.
        queue (str): name of the queue to use when executing the job. The
            default value is the default queue.
    Raises:
        TypeError: When one or more parameters are not JSON serializable.
    Returns:
       str: ID of the schedule message that was sent. None if there was an
           error
    """
    if not class_kwargs:
        class_kwargs = {}
    if not kwargs:
        kwargs = {}

    if not unschedule and \
       ((interval_secs and cron) or (not interval_secs and not cron)):
        logger.error('You must sepcify either `interval_secs` or `cron`, '
                     'but not both (or neither)')
        return

    if func and isinstance(func, basestring):
        if '.' not in func:
            logger.error('Invalid callable string passed, '
                         'absolute path required: "{}"'.format(func))
            return
        path, callable_name = split_callable_name(func)
    elif callable(func):
        callable_name = name_from_callable(func)
        path, callable_name = split_callable_name(callable_name)
    else:
        logger.error('Encountered non-callable func: {}'.format(func))
        return

    if not callable_name or not path:
        logger.error('Encountered invalid callable, will not proceed.')
        return

    # TODO: convert all the times to seconds for the clock
    msg = ['run', {
        'callable': callable_name,
        'path': path,
        'args': args,
        'kwargs': kwargs,
        'class_args': class_args,
        'class_kwargs': class_kwargs,
    }]

    msgid = send_schedule_request(socket, interval_secs=interval_secs or -1,
                                  cron=cron or '',
                                  message=msg, headers=headers, queue=queue,
                                  unschedule=unschedule)

    # TODO: Return msgid only if we got some sort of ACK
    return msgid


def defer_job(
        socket, func, args=(), kwargs=None, class_args=(),
        class_kwargs=None, reply_requested=False, guarantee=False,
        retry_count=0, timeout=0, debounce_secs=False,
        queue=conf.DEFAULT_QUEUE_NAME):
    """
    Used to send a job to a worker to execute via `socket`.

    This tries not to raise any exceptions so use some of the message flags to
    guarentee things.

    .. note::

       All passed class & fuction kwargs/args MUST be json serializable.

    Args:
        socket (socket): eventmq socket to use for sending the message
        func (callable or str): the callable (or string path to callable) to be
            deferred to a worker
        args (list): list of *args for the callable
        kwargs (dict): dict of **kwargs for the callable
        class_args (list): list of *args to pass to the the class when
            initializing (if applicable).
        class_kwargs (dict): dict of **kwargs to pass to the class when
            initializing (if applicable).
        reply_requested (bool): request the return value of func as a reply
        retry_count (int): How many times should be retried when encountering
            an Exception or some other failure before giving up. (default: 0
            or immediately fail)
        timeout (int): How many seconds should we wait before killing the job
            default: 0 which means infinite timeout
        debounce_secs (secs): Number of seconds to debounce the job.   See
            `debounce_deferred_job` for more information.
        queue (str): Name of queue to use when executing the job. If this value
            evaluates to False, the default is used. Default: is configured
            default queue name
    Raises:
        TypeError: When one or more parameters are not JSON serializable.
    Returns:
        str: ID for the message/deferred job. This value will be None if there
            was an error.
    """
    callable_name = None
    path = None

    # Just incase this was passed None
    if not queue:
        queue = conf.DEFAULT_QUEUE_NAME

    if not class_kwargs:
        class_kwargs = {}

    if not kwargs:
        kwargs = {}

    if func and isinstance(func, basestring):
        if '.' not in func:
            logger.error('Invalid callable string passed, '
                         'absolute path required: "{}"'.format(func))
            return
        path, callable_name = split_callable_name(func)
    elif callable(func):
        callable_name = name_from_callable(func)
        path, callable_name = split_callable_name(callable_name)
    else:
        logger.error('Encountered non-callable func: {}'.format(func))
        return

    if not callable_name or not path:
        logger.error('Encountered invalid callable, will not proceed.')
        return

    msg = ['run', {
        'callable': callable_name,
        'path': path,
        'args': args,
        'kwargs': kwargs,
        'class_args': class_args,
        'class_kwargs': class_kwargs,
    }]

    msgid = send_request(socket, msg,
                         reply_requested=reply_requested,
                         guarantee=guarantee,
                         retry_count=retry_count,
                         timeout=timeout,
                         queue=queue)

    return msgid


def send_request(socket, message, reply_requested=False, guarantee=False,
                 retry_count=0, timeout=0, queue=None):
    """
    Send a REQUEST command.

    Default headers are always all disabled by default. If they are included in
    the headers then they have been enabled.

    To execute a task, the message should be formatted as follows:
    {subcommand(str), {
        # dot path location where callable can be imported. If callable is a
        # method on a class, the class should always come last, and be
        # seperated with a colon. (So we know to instantiate on the receiving
        # end)
        'path': path(str),
        # function or method name to run
        'callable': callable(str),
        # Optional args for callable
        'args': (arg, arg),
        # Optional kwargs for callable
        'kwargs': {'kwarg': kwarg},
        # Optional class args, kwargs
        'class_args': (arg2, arg3),
        'class_kwargs': {'kwarg2': kwarg}

        }
    }
    Args:
        socket: Socket (Sender or Receiver) to use when sending `message`
        message: message to send to `socket`
        reply_requested (bool): request the return value of func as a reply
        guarantee (bool): (Give your best effort) to guarantee that func is
            executed. Exceptions and things will be logged.
        retry_count (int): How many times should be retried when encountering
            an Exception or some other failure before giving up. (default: 0
            or immediately fail)
        timeout (int): How many seconds should we wait before killing the job
            default: 0 which means infinite timeout
        queue (str): Name of queue to use when executing the job. Default: is
            configured default queue name

    Returns:
        str: ID of the message
    """
    headers = []

    if reply_requested:
        headers.append('reply-requested')

    if guarantee:
        headers.append('guarantee')

    if retry_count > 0:
        headers.append('retry-count:%d' % retry_count)

    if timeout > 0:
        headers.append('timeout:%d' % timeout)

    msgid = send_emqp_message(socket, 'REQUEST',
                              (queue or conf.DEFAULT_QUEUE_NAME,
                               ",".join(headers),
                               serialize(message)))

    return msgid


def send_schedule_request(socket, message, interval_secs=-1, headers=(),
                          queue=None, unschedule=False, cron=''):
    """
    Send a SCHEDULE or UNSCHEDULE command.

    Queues a message requesting that something happens on an
    interval for the scheduler.

    Args:
        socket (socket):
        job_schedule (str)
        message: Message to send socket.
        headers (list): List of headers for the message
        queue (str): name of queue the job should be executed in
    Returns:
        str: ID of the message
    """

    if unschedule:
        command = 'UNSCHEDULE'
    else:
        command = 'SCHEDULE'

    msgid = send_emqp_message(socket, command,
                              (queue or conf.DEFAULT_QUEUE_NAME,
                               ','.join(headers),
                               str(interval_secs),
                               serialize(message),
                               cron))

    return msgid


def send_publish_request(socket, topic, message):

    msgid = send_emqp_message(socket, 'PUBLISH',
                              (topic,
                               serialize(message), ))

    return msgid
