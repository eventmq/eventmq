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
from time import sleep
import uuid

from . import constants
from . import exceptions
from . import log
from . import utils
from .poller import Poller, POLLIN
from .sender import Sender
from .utils.messages import send_emqp_message as sendmsg
import utils.messages

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
        self.name = kwargs.get('name', str(uuid.uuid4()))
        self.incoming = Sender()
        self.poller = Poller()

        # Alert us of both incoming and outgoing events
        self.poller.register(self.incoming, POLLIN)

        self.status = constants.STATUS.ready

        # Are we waiting for an acknowledgment for something?
        self.awaiting_ack = False

    def start(self, addr='tcp://127.0.0.1:47291'):
        """
        Connect to `addr` and begin listening for job requests

        Args:
            args (str): connection string to connect to
        """
        self.status = constants.STATUS.connecting
        self.incoming.connect(addr)

        self.awaiting_ack = True

        #while self.awaiting_ack:
        self.send_inform()
        #    sleep(5)

        self.status = constants.STATUS.connected

        while True:
            events = self.poller.poll(1000)

            if events.get(self.incoming) == POLLIN:
                msg = self.incoming.recv_multipart()
                self.process_message(msg)

    def process_message(self, msg):
        """
        Processes a message
        """
        try:
            message = utils.messages.parse_message(msg)
        except exceptions.InvalidMessageError:
            logger.error('Invalid message: %s' % str(msg))
            return

        command = message[0]
        msgid = message[1]
        message = message[2]

        if hasattr(self, "on_%s" % command.lower()):
            logger.debug('Calling on_%s' % command.lower())
            func = getattr(self, "on_%s" % command.lower())
            func(msgid, message)
        else:
            logger.warning('No handler for %s found (tried: %s)' %
                           (command, ('on_%s' % command.lower)))

    def process_job(self, msg):
        pass

    def sync(self):
        pass

    def send_inform(self):
        """
        Send an INFORM frame
        """
        sendmsg(self.incoming, 'INFORM', 'default_queuename')

    def on_ack(self, msgid, message):
        """
        Sets :attr:`awaiting_ack` to False
        """
        logger.info('Recieved ACK')
        self.awaiting_ack = False
