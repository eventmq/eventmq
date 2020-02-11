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
import uuid

import six
import zmq

from .. import conf, constants
from ..utils.classes import ZMQReceiveMixin, ZMQSendMixin
from ..utils.devices import generate_device_name


class FakeDevice(ZMQReceiveMixin, ZMQSendMixin):
    """
    A fake router device so we can test with some of the nice utilities, but
    still allowing manual control
    """
    def __init__(self, type_=zmq.ROUTER):
        super(FakeDevice, self).__init__()

        self.name = generate_device_name()
        self.zsocket = zmq.Context.instance().socket(type_)
        self.zsocket.setsockopt(zmq.IDENTITY, self.name)


def send_raw_INFORM(sock, type_, queues=(conf.DEFAULT_QUEUE_NAME,)):
    """
    Send an INFORM message to a raw ZMQ socket.

    Args:
        sock: Socket to send the message on
        type_: Type of client. One of ``scheduler`` or ``worker``
        queues (list, tuple): List of queues to listen on

    Return:
        str: The message id of the INFORM message
    """
    msgid = str(uuid.uuid4())
    tracker = sock.zsocket.send_multipart((
        b'',
        six.ensure_binary(constants.PROTOCOL_VERSION),
        b'INFORM',
        six.ensure_binary(msgid),
        six.ensure_binary(','.join(queues)),
        six.ensure_binary(type_)
    ), copy=False, track=True)
    tracker.wait(1)

    return msgid


def send_raw_READY(sock):
    """
    Sends a READY message to a raw ZMQ socket.

    Args:
        sock: Socket to send the message on

    Return:

    """
    msgid = str(uuid.uuid4())
    tracker = sock.zsocket.send_multipart((
        b'',
        six.ensure_binary(constants.PROTOCOL_VERSION),
        b'READY',
        six.ensure_binary(msgid)
    ), copy=False, track=True)
    tracker.wait(1)

    return msgid
