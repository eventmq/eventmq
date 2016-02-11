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
:mod:`poller` -- Poller
=======================
Device for polling sockets
"""
import logging
import uuid

import zmq
from zmq import Poller as ZPoller


logger = logging.getLogger(__name__)

POLLIN = zmq.POLLIN
POLLOUT = zmq.POLLOUT
POLLINOUT = zmq.POLLIN | zmq.POLLOUT


class Poller(ZPoller):
    """
    :class:`zmq.Poller` based class.
    """
    def __init__(self, *args, **kwargs):
        """
        See :class:`zmq.Poller`
        """
        super(Poller, self).__init__(*args, **kwargs)
        self.name = str(uuid.uuid4())

        self._sockets = []

    def register(self, socket, flag=0):
        """
        Register a socket to be polled by this poller

        Args:
            socket (socket): The socket object to register
            flags (int): :attr:`POLLIN`, :attr:`POLLOUT`, or
                :attr:`POLLIN`|:attr:`POLLOUT` for both. If undefined the
                socket remains unregistered.
        """
        if not flag:
            logger.warning("Leaving socket %s unregistered because no flag "
                           "was defined" % socket.name)
            return
        logger.debug('Registering %s with poller. (flags:%s)' % (socket.name,
                                                                 flag))
        self._sockets.append(socket)

        super(Poller, self).register(socket.zsocket, flag)

    def unregister(self, socket):
        """
        Unregister a socket from being polled

        Args:
            socket (socket): The socket object to registering
        """
        logger.debug("Unregistering %s from poller" % socket.name)
        if socket not in self._sockets:
            logger.warning("Attempt to unregister unregistered socket from "
                           "poller: socket: %s" % socket.name)

        self._sockets.remove(socket)
        super(Poller, self).unregister(socket.zsocket)

    def poll(self, timeout=1):
        """
        Calling :meth:`zmq.Poller.poll` directly returns a tuple set. This
        method typecasts to a dictionary for convience using the socket object
        as the key. If a socket doesn't appear in the returned dictionary then
        no events happened.

        Args:
            timeout (int): How long should poller wait for before iterating
                the next loop.

        Returns (dict) Dictionary using the socket as the key and the event the
            socket generated as the value
        """
        events = dict(super(Poller, self).poll(timeout))
        return_events = {}

        for socket in self._sockets:
            if socket.zsocket not in events:
                continue

            return_events[socket] = events[socket.zsocket]

        return return_events
