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
:mod:`jobs` -- Client Job Helpers
=================================
"""
import logging
import os

from . import messages
from .. import conf
from ..constants import ENV_BROKER_ADDR
from ..exceptions import ConnectionError
from ..sender import Sender

logger = logging.getLogger(__name__)


class Job(object):
    """
    Defines a deferred EventMQ job.

    .. note::

       All passed class & function kwargs/args MUST be json serializable.

    Usage:

    .. code-block:: python

       from eventmq import job

       @job(queue='messaging')
       def send_email(recipient, subject, message):
           from email.mime.text import MIMEText
           import smtplib

           msg = MIMEText(message)
           msg['Subject'] = subject
           msg['From'] = 'no-reply@foobar.io'
           msg['To'] = recipient

           s = smtplib.SMTP('smtp.gmail.com')

           s.login('me@gmail.com', 'my-app-password')

           s.sendmail('me@gmail.com', [recipient,], msg.as_string())
           s.quit()
    """
    def __init__(self, broker_addr=None, queue=None, async_=True, *args,
                 **kwargs):
        """
        Args:
            queue (str): Name of the queue this function should be executed
                in. If no queue provided ``default`` is used.
            broker_addr (str): Address of the broker to send the job to. If no
                address is given then the value of the environment variable
                ``EMQ_BROKER_ADDR`` will be used, If that is undefined a
                warning will be emitted and the job will be run synchronously.
            async_ (bool): If you want to run all executions of a particular
                job synchronously but still decorate it with the job decorator
                you can set this to False. This is useful for unit tests.

        """
        # conf.BROKER_ADDR isn't used because /etc/eventmq.conf is for the
        # daemons.
        self.broker_addr = broker_addr or os.environ.get(ENV_BROKER_ADDR)
        self.queue = queue
        self.async_ = async_

    def __call__(self, f):
        def delay(*args, **kwargs):
            if self.async_ and self.broker_addr:
                socket = Sender()
                socket.connect(addr=self.broker_addr)

                msgid = messages.defer_job(
                    socket, f, args=args, kwargs=kwargs, queue=self.queue)

                return msgid
            else:
                if self.async_ and not self.broker_addr:
                    logger.warning('No EMQ_BROKER_ADDR defined. Running '
                                   'function `{}` synchronously'.format(
                                       f.__name__))
                return f(*args, **kwargs)
        f.delay = delay

        return f


def job(func, broker_addr=None, queue=None, async_=True, *args,
        **kwargs):
    """
    Functional decorator helper for creating a deferred eventmq job. See
    :class:`Job` for more information.
    """
    decorator = Job(queue=queue, broker_addr=broker_addr, async_=async_)

    if callable(func):
        return decorator(func)
    else:
        return decorator


def schedule(func, broker_addr=None, interval_secs=None, args=(), kwargs=None,
             class_args=(), class_kwargs=None, headers=('guarantee',),
             queue=conf.DEFAULT_QUEUE_NAME, cron=None):
    """
    Execute a task on a defined interval.

    .. note::

       All passed class & function kwargs/args MUST be json serializable.

    Args:
        func (callable): the callable (or string path to calable) to be
            scheduled on a worker
        broker_addr (str): Address of the broker to send the job to. If no
            address is given then the value of the environment variable
            ``EMQ_BROKER_ADDR`` will be used.
        interval_secs (int): Run job every interval_secs or None if using cron
        args (list): list of *args to pass to the callable
        kwargs (dict): dict of **kwargs to pass to the callable
        class_args (list): list of *args to pass to the class (if applicable)
        class_kwargs (dict): dict of **kwargs to pass to the class (if
            applicable)
        headers (list): list of strings denoting enabled headers. Default:
            guarantee is enabled to ensure the scheduler schedules the job.
        queue (str): name of the queue to use when executing the job. The
            default value is the default queue.
        cron (string): cron formatted string used for job schedule if
            interval_secs is None, i.e. '* * * * *' (every minute)
    Raises:
        TypeError: When one or more parameters are not JSON serializable.
    Returns:
       str: ID of the schedule message that was sent. None if there was an
           error
    """
    socket = Sender()
    # conf.BROKER_ADDR isn't used because /etc/eventmq.conf is for the daemons.
    broker_addr = broker_addr or os.environ.get(ENV_BROKER_ADDR)

    socket.connect(broker_addr)

    if not broker_addr:
        raise ConnectionError('unknown broker address: {}'.format(broker_addr))

    return messages.schedule(
        socket, func, interval_secs=interval_secs, args=args,
        kwargs=kwargs, class_args=class_args, class_kwargs=class_kwargs,
        headers=headers, queue=conf.DEFAULT_QUEUE_NAME, cron=cron)


def unschedule(func, broker_addr=None, interval_secs=None, args=(),
               kwargs=None, class_args=(), class_kwargs=None,
               headers=('guarantee',), queue=conf.DEFAULT_QUEUE_NAME,
               cron=None):
    """
    Stop periodically executing a task

    .. note::

       All passed class & function kwargs/args MUST be json serializable.

    Args:
        func (callable): the callable (or string path to calable) to be
            scheduled on a worker
        broker_addr (str): Address of the broker to send the job to. If no
            address is given then the value of the environment variable
            ``EMQ_BROKER_ADDR`` will be used.
        interval_secs (int): Run job every interval_secs or None if using cron
        args (list): list of *args to pass to the callable
        kwargs (dict): dict of **kwargs to pass to the callable
        class_args (list): list of *args to pass to the class (if applicable)
        class_kwargs (dict): dict of **kwargs to pass to the class (if
            applicable)
        headers (list): list of strings denoting enabled headers. Default:
            guarantee is enabled to ensure the scheduler schedules the job.
        queue (str): name of the queue to use when executing the job. The
            default value is the default queue.
        cron (string): cron formatted string used for job schedule if
            interval_secs is None, i.e. '* * * * *' (every minute)
    Raises:
        TypeError: When one or more parameters are not JSON serializable.
    Returns:
       str: ID of the schedule message that was sent. None if there was an
           error
    """
    socket = Sender()
    # conf.BROKER_ADDR isn't used because /etc/eventmq.conf is for the daemons.
    broker_addr = broker_addr or os.environ.get(ENV_BROKER_ADDR)
    socket.connect(broker_addr)

    if not broker_addr:
        raise ConnectionError('unknown broker address: {}'.format(broker_addr))

    socket.connect(addr=broker_addr)

    return messages.schedule(
        socket, func, interval_secs=interval_secs, args=args,
        kwargs=kwargs, class_args=class_args, class_kwargs=class_kwargs,
        headers=headers, queue=conf.DEFAULT_QUEUE_NAME, cron=cron,
        unschedule=True)
