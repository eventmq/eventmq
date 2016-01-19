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
:mod:`messages` -- Client Messaging
===================================
"""
import inspect
import logging
from json import dumps as serialize

from .. import conf
from ..utils.messages import send_emqp_message

logger = logging.getLogger(__name__)


def schedule(socket, func, interval_secs, args=(), kwargs=None, class_args=(),
             class_kwargs=None, headers=('guarantee',),
             queue=conf.DEFAULT_QUEUE_NAME, unschedule=False):
    """
    Execute a task on a defined interval.

    Args:
        socket (socket): eventmq socket to use for sending the message
        func (callable): the callable to be scheduled on a worker
        minutes (int): minutes to wait in between executions
        args (list): list of *args to pass to the callable
        kwargs (dict): dict of **kwargs to pass to the callable
        class_args (list): list of *args to pass to the class (if applicable)
        class_kwargs (dict): dict of **kwargs to pass to the class (if
            applicable)
        headers (list): list of strings denoting enabled headers. Default:
            guarantee is enabled to ensure the scheduler schedules the job.
        queue (str): name of the queue to use when executing the job. The
            default value is the default queue.
    """
    if not class_kwargs:
        class_kwargs = {}
    if not kwargs:
        kwargs = {}

    if callable(func):
        path, callable_name = build_module_path(func)
    else:
        logger.error('Encountered non-callable func: {}'.format(func))
        return False

    if not callable_name:
        logger.error('Encountered callable with no name in {}'.format(
            func.__module__
        ))
        return False

    if not path:
        logger.error('Encountered callable with no __module__ path {}'.format(
            func.__name__
        ))
        return False

    # TODO: convert all the times to seconds for the clock

    # TODO: send the schedule request

    msg = ['run', {
        'callable': callable_name,
        'path': path,
        'args': args,
        'kwargs': kwargs,
        'class_args': class_args,
        'class_kwargs': class_kwargs,
    }]

    send_schedule_request(socket, interval_secs=interval_secs,
                          message=msg, headers=headers, queue=queue,
                          unschedule=unschedule)


def defer_job(socket, func, args=(), kwargs=None, class_args=(),
              class_kwargs=None, reply_requested=False, guarantee=False,
              retry_count=0, queue=conf.DEFAULT_QUEUE_NAME):
    """
    Used to send a job to a worker to execute via `socket`.

    This tries not to raise any exceptions so use some of the message flags to
    guarentee things.

    Args:
        socket (socket): eventmq socket to use for sending the message
        func (callable): the callable to be deferred to a worker
        args (list): list of *args for the callable
        kwargs (dict): dict of **kwargs for the callable
        class_args (list): list of *args to pass to the the class when
            initializing (if applicable).
        class_kwargs (dict): dict of **kwargs to pass to the class when
            initializing (if applicable).
        reply_requested (bool): request the return value of func as a reply
        guarantee (bool): (Give your best effort) to guarantee that func is
            executed. Exceptions and things will be logged.
        retry_count (int): How many times should be retried when encountering
            an Exception or some other failure before giving up. (default: 0
            or immediatly fail)
        queue (str): Name of queue to use when executing the job. Default: is
            configured default queue name
    Returns:
        bool: True if the message was successfully queued, False if something
        went wrong. If something did go wrong check the logs for details.
    """
    callable_name = None
    path = None

    if not class_kwargs:
        class_kwargs = {}
    if not kwargs:
        kwargs = {}

    if callable(func):
        path, callable_name = build_module_path(func)

    else:
        logger.error('Encountered non-callable func: {}'.format(func))
        return False

    # Check for and log errors
    if not callable_name:
        logger.error('Encountered callable with no name in {}'.
                     format(func.__module__))
        return False

    if not path:
        logger.error('Encountered callable with no __module__ path {}'.
                     format(func.__name__))
        return False

    msg = ['run', {
        'callable': callable_name,
        'path': path,
        'args': args,
        'kwargs': kwargs,
        'class_args': class_args,
        'class_kwargs': class_kwargs,
    }]

    send_request(socket, msg, reply_requested=reply_requested,
                 guarantee=guarantee, retry_count=retry_count, queue=queue)

    return True  # The message has successfully been queued for delivery


def build_module_path(func):
    """
    Builds the module path in string format for a callable.

    .. note:
       To use a callable Object, pass Class.__call__ as func and provide any
       class_args/class_kwargs. This is so side effects from pickling won't
       occur.

    Args:
        func (callable): The function or method to build the path for
    Returns:
        list: (import path (w/ class seperated by a ':'), callable name) or
        (None, None) on error
    """
    callable_name = None

    path = None
    # Methods also have the func_name property
    if inspect.ismethod(func):
        path = ("{}:{}".format(func.__module__, func.im_class.__name__))
        callable_name = func.func_name
    elif inspect.isfunction(func):
        path = func.__module__
        callable_name = func.func_name
    else:
        # We should account for another callable type so log information
        # about it
        if hasattr(func, '__class__') and isinstance(func, func.__class__):
            func_type = 'instanceobject'
        else:
            func_type = type(func)

        logger.error('Encountered unknown callable ({}) type {}'.format(
            func,
            func_type
        ))
        return None, None

    return path, callable_name


def send_request(socket, message, reply_requested=False, guarantee=False,
                 retry_count=0, queue=None):
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
        socket (socket): Socket to use when sending `message`
        message: message to send to `socket`
        reply_requested (bool): request the return value of func as a reply
        guarantee (bool): (Give your best effort) to guarantee that func is
            executed. Exceptions and things will be logged.
        retry_count (int): How many times should be retried when encountering
            an Exception or some other failure before giving up. (default: 0
            or immediatly fail)
        queue (str): Name of queue to use when executing the job. Default: is
            configured default queue name
    """
    headers = []

    if reply_requested:
        headers.append('reply-requested')

    if guarantee:
        headers.append('guarantee')

    if retry_count > 0:
        headers.append('retry-count:%d' % retry_count)

    send_emqp_message(socket, 'REQUEST',
                      (queue or conf.DEFAULT_QUEUE_NAME,
                       ",".join(headers),
                       serialize(message))
                      )


def send_schedule_request(socket, interval_secs, message, headers=(),
                          queue=None, unschedule=False):
    """
    Send a SCHEDULE or UNSCHEDULE command.

    Queues a message requesting that something happens on an
    interval for the scheduler.

    Args:
        socket (socket):
        interval_secs (int):
        message: Message to send socket.
        headers (list): List of headers for the message
        queue (str): name of queue the job should be executed in
    """

    if unschedule:
        command = 'UNSCHEDULE'
    else:
        command = 'SCHEDULE'

    send_emqp_message(socket, command,
                      (queue or conf.DEFAULT_QUEUE_NAME,
                       ','.join(headers),
                       str(interval_secs),
                       serialize(message)))


def job(block=False):  # Move to decorators.py
    """
    run the decorated function on a worker

    Args:
        block (bool): Set to True if you wish to block and wait for the
            response. This may be useful for running quick but cpu intesive
            that would otherwise overwhelm a box that has to do it all alone.
            (decryption?)
    """
    raise NotImplementedError('eventmq.client.messages.job')
