"""
Receiver
========
The receiver is responsible for
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

import log

logger = log.get_logger(__file__)


class Receiver(object):
    """
    Receives messages

    :attr name: Name of this socket
    :attr zcontext:  :class:`zmq.Context`
    :attr zsocket: :class`zmq.Socket`
    """
    def __init__(self, *args, **kwargs):
        """
        :param name: name of this socket
        :type name: str
        :param context: context to use to build the socket
        :type context: :class:`zmq.Context`
        :param socket: socket (should be :attr:`zmq.REP` or :attr:`zmq.ROUTER`
            type)
        :type socket: :class:
        """
        self.name = kwargs.get('name', uuid.uuid4())
        self.zcontext = kwargs.get('context', zmq.Context.instance())
        self.zsocket = kwargs.get('socket', self.zcontext.socket(zmq.ROUTER))

        self.callable = kwargs.get('callable')
        if not callable(self.callable):
            raise TypeError('Required argument "callable" is not actually '
                            'callable')

    def listen(self, addr=None):
        """
        start listening on `addr`

        :param addr: Address to listen on as a connction string
        :type addr: str
        """
        self.zsocket.bind(addr)
        self.logger.info('Receiver %s: Listening on %s' % (self.name, addr))
