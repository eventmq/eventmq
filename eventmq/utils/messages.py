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
:mod:`messages` -- Message Utilities
==========================================
"""
import logging

import six

from . import random_characters
from .. import conf, constants, exceptions

logger = logging.getLogger(__name__)


def parse_router_message(message):
    """
    Parses the generic format of an eMQP/1.0 message and returns the
    parts.

    Args:
        message: the message you wish to have parsed

    Returns (tuple) (sender_id, command, message_id, (message_body, and_data))
    """
    try:
        sender = message[0]
        # noop = message[1]
        # protocol_version = message[2]
        command = message[3]
        msgid = message[4]
    except IndexError:
        raise exceptions.InvalidMessageError('Invalid Message Encountered: %s'
                                             % str(message))
    if len(message) > 5:
        msg = message[5:]
    else:
        msg = ()
    return (sender, command, msgid, msg)


def parse_message(message):
    """
    Parses the generic format of an eMQP/1.0 message and returns the
    parts.

    Args:
        message: the message you wish to have parsed

    Returns (tuple) (command, message_id, (message_body, and_data))
    """
    try:
        # noop = message[0]
        # protocol_version = message[1]
        command = message[2]
        msgid = message[3]
    except IndexError:
        raise exceptions.InvalidMessageError('Invalid Message Encountered: %s'
                                             % str(message))
    if len(message) > 4:
        msg = message[4:]
    else:
        msg = ()
    return (command, msgid, msg)


def generate_msgid(prefix=None):
    """
    Returns a random string to be used for message ids. Optionally the ID can
    be prefixed with `prefix`.

    Args:
        prefix (str): Value to prefix on to the random part of the id. Useful
            for prefixing some meta data to use for things
    """
    id = random_characters()
    return id if not prefix else str(prefix) + id


def send_emqp_message(socket, command, message=None):
    """
    Formats and sends an eMQP message

    Args:
        socket
        command
        message
    Raises:

    Returns:
        str: Message id for this message
    """
    msgid = generate_msgid()
    msg = (str(command).upper(), msgid)
    if message and isinstance(message, (tuple, list)):
        msg += tuple(message)
    elif message:
        msg += (message,)

    socket.send_multipart(msg, constants.PROTOCOL_VERSION)

    return msgid


def send_emqp_router_message(socket, recipient_id, command, message=None):
    """
    Formats and sends an eMQP message taking into account the recipient frame
    used by a :attr:`zmq.ROUTER` device.

    Args:
        socket: socket to send the message with
        recipient_id (str): the id of the connected device to reply to
        command (str): the eMQP command to send
        message: a msg tuple to send

    Returns
        str: Message id for this message
    """
    msgid = generate_msgid()
    msg = (str(command).upper(), msgid)
    if message and isinstance(message, (tuple, list)):
        msg += message
    elif message:
        msg += (message,)

    socket.send_multipart(msg, constants.PROTOCOL_VERSION,
                          _recipient_id=recipient_id)

    return msgid


def fwd_emqp_router_message(socket, recipient_id, payload):
    """
    Forwards `payload` to socket untouched.

    .. note:
       Because it's untouched, and because this function targets
       :prop:`zmq.ROUTER`, it may be a good idea to first strip off the
       leading sender id before forwarding it. If you dont you will need to
       account for that on the recipient side.

    Args:
        socket: socket to send the message with
        recipient_id (str): the id of the connected device to reply to
        payload (list): The message to send. The first frame should be an
            empty string
    """
    import zmq
    import errno

    errnos = [errno.EHOSTUNREACH,
              errno.EAGAIN]

    payload = [recipient_id, ] + payload
    if conf.SUPER_DEBUG:
        logger.debug('Forwarding message: {}'.format(str(payload)))
    try:
        socket.zsocket.send_multipart([six.ensure_binary(x) for x in payload],
                                      flags=zmq.NOBLOCK)
    except zmq.error.ZMQError as e:
        if e.errno in errnos:
            e_message = str(e) + " {}".format(recipient_id)
            raise exceptions.PeerGoneAwayError(e_message)
        else:
            raise exceptions.EventMQError("errno {}: {}".format(e.errno,
                                                                str(e)))
