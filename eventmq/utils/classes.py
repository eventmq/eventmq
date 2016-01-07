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
:mod:`classes` -- Utility Classes
=================================
Defines some classes to use when implementing ZMQ devices
"""
import logging

import zmq.error

from .. import conf, exceptions
from ..utils.messages import send_emqp_message as sendmsg
from ..utils.timeutils import monotonic, timestamp

logger = logging.getLogger(__name__)


class HeartbeatMixin(object):
    """
    Provides methods for implementing heartbeats
    """
    def __init__(self, *args, **kwargs):
        """
        Sets up some variables to track the state of heartbeaty things
        """
        if not hasattr(self, '_meta'):
            self._meta = {}

        self.reset_heartbeat_counters()

    def reset_heartbeat_counters(self):
        """
        Resets all the counters for heartbeats back to 0
        """
        # the monotonic clock is used for the interval values like
        # 'last_sent_heartbeat'
        self._meta['last_sent_heartbeat'] = 0
        self._meta['last_received_heartbeat'] = 0
        self._meta['heartbeat_miss_count'] = 0

    def send_heartbeat(self, socket):
        """
        Send a HEARTBEAT command to the specified socket

        Args:
            socket (socket): The eMQP socket to send the message to
        """
        # Note: When updating this function, also make sure the custom versions
        # acts as expected in router.py
        sendmsg(socket, 'HEARTBEAT', str(timestamp()))
        self._meta['last_sent_heartbeat'] = monotonic()

    def is_dead(self, now=None):
        """
        Checks the heartbeat counters to find out if the thresholds have been
        met.

        Args:
            now (float): The time to use to check if death has occurred. If
                this value is None, then :func:`utils.timeutils.monotonic`
                is used.

        Returns:
            bool: True if the connection to the peer has died, otherwise
                False
        """
        if not now:
            now = monotonic()

        if now - self._meta['last_received_heartbeat'] >= \
           conf.HEARTBEAT_TIMEOUT:
            self._meta['heartbeat_miss_count'] += 1
            self._meta['last_received_heartbeat'] = now

            if self._meta['heartbeat_miss_count'] >= \
               conf.HEARTBEAT_LIVENESS:
                return True

        return False


class ZMQReceiveMixin(object):
    """
    Defines some methods for receiving messages. This class will not work if
    used on it's own
    """
    def recv(self):
        """
        Receive a message
        """
        msg = self.zsocket.recv()
        if conf.SUPER_DEBUG:
            if not ("HEARTBEAT" == msg[2] or "HEARTBEAT" == msg[3]) or \
               not conf.HIDE_HEARTBEAT_LOGS:
                logger.debug('Received message: {}'.format(msg))
        return msg

    def recv_multipart(self):
        """
        Receive a multipart message
        """
        msg = self.zsocket.recv_multipart()
        if conf.SUPER_DEBUG:
            # If it's not at least 4 frames long then most likely it isn't an
            # eventmq message
            if len(msg) >= 4 and \
               not ("HEARTBEAT" == msg[2] or "HEARTBEAT" == msg[3]) or \
               not conf.HIDE_HEARTBEAT_LOGS:
                logger.debug('Received message: {}'.format(msg))
        return msg


class ZMQSendMixin(object):
    """
    Defines some methods for sending messages. This class will not work if used
    on it's own
    """
    def send_multipart(self, message, protocol_version, _recipient_id=None):
        """
        Send a message directly to the 0mq socket. Automatically inserts some
        frames for your convience. The sent frame ends up looking something
        like this

            (_recipient_id, '', protocol_version) + (your, tuple)

        Args:
            message (tuple): Raw message to send.
            protocol_version (str): protocol version. it's good practice but
                you may explicitly specify None to skip adding the version
            _recipient_id (object): When using a :attr:`zmq.ROUTER` you must
                specify the the recipient id of the remote socket
        """
        supported_msg_types = (tuple, list)
        if not isinstance(message, supported_msg_types):
            raise exceptions.MessageError(
                '%s message type not one of %s' %
                (type(message), str(supported_msg_types)))

        if isinstance(message, list):
            message = tuple(message)

        if _recipient_id:
            headers = (_recipient_id, '', protocol_version)
        else:
            headers = ('', protocol_version, )

        msg = headers + message

        if conf.SUPER_DEBUG:
            # If it's not at least 4 frames long then most likely it isn't an
            # eventmq message
            if len(msg) == 4 and \
               not ("HEARTBEAT" == msg[2] or "HEARTBEAT" == msg[3]) or \
               not conf.HIDE_HEARTBEAT_LOGS:
                logger.debug('Sending message: %s' % str(msg))

        try:
            self.zsocket.send_multipart(msg)
        except zmq.error.ZMQError as e:
            if 'No route' in e.message:
                raise exceptions.PeerGoneAwayError(e)

    def send(self, message, protocol_version):
        """
        Sends a message

        Args:
            message: message to send to something
            protocol_version (str): protocol version. it's good practice, but
                you may explicitly specify None to skip adding the version
        """
        self.send_multipart((message, ), protocol_version)
