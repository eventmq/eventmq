"""
:mod:`receiver` -- Receiver
===========================
The receiver is responsible for receiveing messages
"""
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

import zmq
from zmq.eventloop import zmqstream

import eventmq
import log

logger = log.get_logger(__file__)


class Receiver(object):
    """
    Receives messages and pass them to a callable.

    .. note::
       Polling with this reciever is currently only available via an eventloop
       (:mod:`zmq.eventloop`).

    Attributes:
        name (str): Name of this socket
        zcontext (:class:`zmq.Context`): socket context
        zsocket (:class:`zmq.Socket`): socket wrapped up in a
            :class:`zmqstream.ZMQStream`
    """

    def __init__(self, *args, **kwargs):
        """
        .. note::
           All args are optional unless otherwise noted.

        Args:
            callable: REQUIRED A function or method to call when a message is
                received
            name (str): name of this socket. By default a uuid will be
                generated
            context (:class:`zmq.Context`): Context to use when buliding the
                socket
            socket (:class:`zmq.Socket`): Should be one of :attr:`zmq.REP` or
                :attr:`zmq.ROUTER`. By default a `ROUTER` is used
            skip_zmqstream (bool): If set to true, skip creating the zmqstream
                socket. Callable is unused and optional when this is True
        Raises:
            :class:`TypeError`: when `callable` is not callable
        """
        self.zcontext = kwargs.get('context', zmq.Context.instance())
        self.name = kwargs.get('name', str(uuid.uuid4()))
        self.skip_zmqstream = kwargs.get('skip_zmqstream', False)
        self.callable = kwargs.get('callable')

        self.zsocket = kwargs.get('socket', self.zcontext.socket(zmq.ROUTER))
        self.zsocket.setsockopt(zmq.IDENTITY, self.name)

        if not self.skip_zmqstream and not callable(self.callable):
            raise TypeError('Required argument "callable" is not actually '
                            'callable')

        if not self.skip_zmqstream:
            logger.debug('Using ZMQStream')
            self.zsocket = zmqstream.ZMQStream(self.zsocket)

            self.zsocket.on_recv(self.callable)

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
            self.status = eventmq.STATUS.connected
            logger.info('Receiver %s: Connected to %s' % (self.name, addr))
        else:
            raise Exception('Receiver %s not ready. status=%s' %
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
