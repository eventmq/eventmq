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


def defer_job(socket, func, args=(), kwargs=None, class_args=(),
              class_kwargs=None, reply_requested=False, guarantee=False,
              retry_count=0, queue=None):
    """
    sends a job to a worker to execute.

    This should not raise any exceptions, so always check your response.
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
        return

    # Check for and log errors
    if not callable_name:
        logger.error('Encountered callable with no name in {}'.
                     format(func.__module__))
        return

    if not path:
        logger.error('Encountered callable with no __module__ path {}'.
                     format(func.func_name))
        return

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


def build_module_path(func):
    """
    Builds the module path in string format for a callable.

    .. note:
       To use a callable Object, pass Class.__call__ as func and provide any
       class_args/class_kwargs. This is so side effects from pickling won't
       occur.

    Args:
        func (callable): The function or method to build the path for

    Returns (list): (import path (w/ class seperated by a ':'), callable name)
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


def job(block=False):  # Move to decorators.py
    """
    run the decorated function on a worker

    Args:
        block (bool): Set to True if you wish to block and wait for the
            response. This may be useful for running quick but cpu intesive
            that would otherwise overwhelm a box that has to do it all alone.
            (decryption?)
    """
