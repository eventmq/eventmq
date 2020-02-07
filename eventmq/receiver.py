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
:mod:`receiver` -- Receiver
===========================
The receiver is responsible for receiveing messages
"""
import logging

import six
import zmq

from . import conf, constants
from .utils.classes import ZMQReceiveMixin, ZMQSendMixin
from .utils.devices import generate_device_name


logger = logging.getLogger(__name__)


class Receiver(ZMQReceiveMixin, ZMQSendMixin):
    """
    Receives messages and pass them to a on_recv.

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
            context (:class:`zmq.Context`): Context to use when buliding the
                socket
            socket (:class:`zmq.Socket`): Should be one of :attr:`zmq.REP` or
                :attr:`zmq.ROUTER`. By default a `ROUTER` is used
        Raises:
            :class:`TypeError`: when `callable` is not callable
        """
        from .utils import settings
        settings.import_settings()

        self.zcontext = kwargs.get('context', zmq.Context.instance())
        self.zcontext.set(zmq.MAX_SOCKETS, conf.MAX_SOCKETS)

        self.name = six.ensure_binary(kwargs.get('name',
                                                 generate_device_name()))

        self.zsocket = kwargs.get('socket', self.zcontext.socket(zmq.ROUTER))
        self.zsocket.setsockopt(zmq.IDENTITY, self.name)
        self.zsocket.setsockopt(zmq.ROUTER_MANDATORY, 1)

        self.status = constants.STATUS.ready

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
            logger.info('Receiver %s: Listening on %s' % (self.name, addr))
        else:
            raise Exception('Receiver %s not ready. status=%s' %
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
            self.status = constants.STATUS.connected
            logger.debug('Receiver %s: Connected to %s' % (self.name, addr))
        else:
            raise Exception('Receiver %s not ready. status=%s' %
                            (self.name, self.status))

    def unbind(self, addr):
        """
        Unbinds current socket

        Args:
            addr (str): Address to disconnect from as a string

        Raises:
           :class:`Exception`
        """

        if self.status == constants.STATUS.listening:
            self.zsocket.unbind(addr)
            self.status = constants.STATUS.ready
        else:
            raise Exception('Receiver %s is %s, but is trying to unbind'
                            % (self.name, self.status))

    @property
    def ready(self):
        """
        Property used to check if this receiver is ready.

        Returns:
            bool: True if the receiver is ready to connect or listen, otherwise
                False
        """
        return self.status == constants.STATUS.ready
