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
:mod:`sender` -- Sender
=======================
The sender is responsible for sending messages
"""
import uuid

import zmq
from zmq.eventloop import zmqstream

from . import eventmq
from . import exceptions
from . import log

logger = log.get_logger(__file__)


class Sender(object):
    """
    Sends messages to a particular socket

    .. note::
       Polling with this sender is currently only available via an eventloop
       (:mod:`zmq.eventloop`)

    Attributes:
        name (str): Name of this socket
        zcontext (:class`zmq.Context`): socket context
        zsocket (:class:`zmq.Socket`): socket wrapped up in a
            :class:`zmqstream.ZMQStream`
    """

    def __init__(self, *args, **kwargs):
        """
        .. note::
           All args are optional unless otherwise noted.

        Args:
            name (str): name of this socket. By default a uuid will be
                generated
            context (:class:`zmq.Context`): Context to use when building the
                socket
            socket (:class:`zmq.Socket`): Should be one of :attr:`zmq.REQ` or
                :attr:`zmq.DEALER`. By default a `DEALER` is used
            skip_zmqstream (bool): If set to true, skip creating the zmqstream
                socket

        """
        self.name = kwargs.get('name', str(uuid.uuid4()))
        self.zcontext = kwargs.get('context', zmq.Context.instance())

        self.zsocket = kwargs.get('socket', self.zcontext.socket(zmq.DEALER))
        self.zsocket.setsockopt(zmq.IDENTITY, self.name)

        if not kwargs.get('skip_zmqstream', True):
            logger.debug('Using ZMQStream')
            self.zsocket = zmqstream.ZMQStream(self.zsocket)
            self.zsocket.on_recv(kwargs.get('on_recv'))

        self.status = eventmq.STATUS.ready

    def listen(self, addr=None):
        """
        start listening on `addr`

        Args:
            addr (str): Address to listen on as a connction string

        Raises:
            :class:`Exception`
        """
        if self.ready:
            self.zsocket.bind(addr)
            self.status = eventmq.STATUS.listening
            logger.info('Receiver %s: Listening on %s' % (self.name, addr))
        else:
            raise exceptions.EventMQError('Receiver %s not ready. status=%s' %
                                          (self.name, self.status))

    def connect(self, addr=None):
        """
        Connect to address defined by `addr`

        Args:
            addr (str): Address to connect to as a connection string

        Raises:
            :class:`Exception`
        """
        if self.ready:
            self.zsocket.connect(addr)
            self.status = eventmq.STATUS.connected
            logger.info('Receiver %s: Connected to %s' % (self.name, addr))
        else:
            raise exceptions.EventMQError('Receiver %s not ready. status=%s' %
                                          (self.name, self.status))

    @property
    def ready(self):
        """
        Property used to check if this receiver is ready.

        Returns:
            bool: True if the receiver is ready to connect or listen, otherwise
                False
        """
        return self.status == eventmq.STATUS.ready

    def send_multipart(self, message, protocol_version):
        """
        Send a message directly to the 0mq socket. Automatically inserts some
        frames for your convience. The sent frame ends up looking something
        like identity

            (this, '', protocol_version) + (your, tuple)

        Args:
            message (tuple): Raw message to send.
            protocol_version (str): protocol version. it's good practice but
                you may explicitly specify None to skip adding the version
        """
        supported_msg_types = (tuple, list)
        if not isinstance(message, supported_msg_types):
            raise exceptions.MessageError(
                '%s message type not one of %s' %
                (type(message), str(supported_msg_types)))

        if isinstance(message, list):
            message = tuple(message)

        headers = ('', protocol_version, )

        message = headers + message
        print message
        self.zsocket.send_multipart(message)

    def send(self, message, protocol_version):
        """
        Sends a message

        Args:
            message: message to send to something
            protocol_version (str): protocol version. it's good practice, but
                you may explicitly specify None to skip adding the version
        """
        logger.debug('Sending message: %s' % str(message))
        self.send_multipart((message, ), protocol_version)

    def recv(self):
        """
        Receive a message
        """
        return self.zsocket.recv()

    def recv_multipart(self):
        """
        Receive a multipart message
        """
        return self.zsocket.recv_multipart
