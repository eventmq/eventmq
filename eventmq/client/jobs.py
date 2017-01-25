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
from ..constants import ENV_BROKER_ADDR
from ..sender import Sender

logger = logging.getLogger(__name__)


class Job(object):
    """
    Defines a deferred EventMQ job.

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
    def __init__(self, broker_addr=None, queue=None, async=True, *args,
                 **kwargs):
        """
        Args:
            queue (str): Name of the queue this function should be executed
                in. If no queue provided ``default`` is used.
            broker_addr (str): Address of the broker to send the job to. If no
                address is given then the value of the environment variable
                ``EMQ_BROKER_ADDR`` will be used, If that is undefined a
                warning will be emitted and the job will be run synchronously.
            async (bool): If you want to run all executions of a particular job
                synchronously but still decorate it with the job decorator you
                can set this to False. This is useful for unit tests.

        """
        self.broker_addr = broker_addr or os.environ.get(ENV_BROKER_ADDR)
        self.queue = queue
        self.async = async

    def __call__(self, f):
        def delay(*args, **kwargs):
            if self.async and self.broker_addr:
                socket = Sender()
                socket.connect(addr=self.broker_addr)

                msgid = messages.defer_job(
                    socket, f, args=args, kwargs=kwargs, queue=self.queue)

                return msgid
            else:
                if self.async and not self.broker_addr:
                    logger.warning('No EMQ_BROKER_ADDR defined. Running '
                                   'function `{}` synchronously'.format(
                                       f.__name__))
                return f(*args, **kwargs)
        f.delay = delay

        return f


def job(func, broker_addr=None, queue=None, async=True, *args, **kwargs):
    """
    Functional decorator helper for creating a deferred eventmq job. See
    :class:`Job` for more information.
    """
    decorator = Job(queue=queue, broker_addr=broker_addr, async=async)

    if callable(func):
        return decorator(func)
    else:
        return decorator
