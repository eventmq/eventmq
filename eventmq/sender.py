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
:mod:`sender` -- Sender
=======================
The sender is responsible for sending messages
"""
import logging
import sys
import uuid

import zmq

from . import conf, constants, exceptions
from .utils.classes import ZMQReceiveMixin, ZMQSendMixin

logger = logging.getLogger(__name__)


class Sender(ZMQSendMixin, ZMQReceiveMixin):
    """
    Sends messages to a particular socket

    Attributes:
        name (str): Name of this socket
        zcontext (:class:`zmq.Context`): socket context
        zsocket (:class:`zmq.Socket`):
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
        from .utils import settings
        settings.import_settings()

        self.zcontext = kwargs.pop('context', zmq.Context.instance())
        self.zcontext.set(zmq.MAX_SOCKETS, conf.MAX_SOCKETS)

        # Set zsocket to none so we can check if it exists and close it before
        # rebuilding it later.
        self.zsocket = None

        self.name = kwargs.pop('name', str(uuid.uuid4()))

        self.rebuild(*args, **kwargs)

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
            self.status = constants.STATUS.listening
            logger.info('Listening on %s' % (addr))
        else:
            raise exceptions.EventMQError('Not ready. status=%s' %
                                          (self.status))

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
            self.status = constants.STATUS.connected
            logger.debug('Connecting to %s' % (addr))
        else:
            raise exceptions.EventMQError('Not ready. status=%s' %
                                          (self.status))

    def unbind(self, addr):
        """
        Unbinds current socket

        Args:
            addr (str): Address to unbind from as a string

        Raises:
           :class:`Exception`
        """

        if self.status == constants.STATUS.listening:
            self.zsocket.unbind(addr)
            self.status = constants.STATUS.ready
        else:
            raise Exception("Receiver {} is {}, but is trying to unbind".format
                            (
                                self.name,
                                self.status))

    def rebuild(self, *args, **kwargs):
        """
        Rebuilds the socket. This is useful when you need to reconnect to
        something without restarting the process. Many of these things happen
        happen during :meth:`self.__init__`, so it takes roughly the same
        parameters as :meth:`self.__init__`

        Args:
            socket (:class:`zmq.Socket`): Should be one of :attr:`zmq.REQ` or
                :attr:`zmq.DEALER`. By default a `DEALER` is used
            skip_zmqstream (bool): If set to true, skip creating the zmqstream
                socket
        """
        if self.zsocket:
            self.zsocket.close()

        self.zsocket = kwargs.pop('socket', self.zcontext.socket(zmq.DEALER))

        self.name = kwargs.pop('name', str(uuid.uuid4()))

        if sys.version[0] == '2':
            self.zsocket.setsockopt(zmq.IDENTITY, self.name)
        else:
            self.zsocket.setsockopt_string(zmq.IDENTITY, str(self.name))

        self.status = constants.STATUS.ready

    @property
    def ready(self):
        """
        Property used to check if this receiver is ready.

        Returns:
            bool: True if the receiver is ready to connect or listen, otherwise
                False
        """
        return self.status == constants.STATUS.ready
