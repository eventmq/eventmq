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
:mod:`jobmanager` -- Job Manager
================================
Ensures things about jobs and spawns the actual tasks
"""
import uuid

from zmq.eventloop import ioloop

from . import constants
from . import log
from . import utils
from .sender import Sender

logger = log.get_logger(__file__)


class JobManager(object):
    """
    The exposed portion of the worker. The job manager's main responsibility is
    to manage the resources on the server it's running.

    This job manager uses tornado's eventloop.
    """

    def __init__(self, *args, **kwargs):
        """
        .. note ::
           All args are optional unless otherwise noted.

        Args:
            name (str): unique name of this instance. By default a uuid will be
                 generated.
        """
        ioloop.install()
        self.name = kwargs.get('name', str(uuid.uuid4()))
        self.incoming = Sender()

        self.status = constants.STATUS.ready

    def start(self, addr='tcp://127.0.0.1:47291'):
        """
        Begin listening for job requests

        Args:
            args (str): connection string to connect to
        """
        self.incoming.connect(addr)
        self.status = constants.STATUS.listening

        self.send_inform()
        ioloop.IOLoop.instance().start()

    def process_job(self, msg):
        pass

    def sync(self):
        pass

    def send_message(self, command, message):
        """
        send a message to `self.incoming`
        Args:
            message: a msg tuple to send
        Raises:

        Returns
        """
        msg = (str(command).upper(), utils.generate_msgid())
        if isinstance(message, (tuple, list)):
            msg += message
        else:
            msg += (message,)

        logger.debug('Sending message: %s' % str(msg))
        self.incoming.send_multipart(msg, constants.PROTOCOL_VERSION)

    def send_inform(self):
        """
        Send an INFORM frame
        """
        self.send_message('INFORM', 'default_queuename')


    def respond(self):
        pass
