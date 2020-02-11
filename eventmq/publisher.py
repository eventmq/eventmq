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
:mod:`publisher` -- Publisher
=======================
Publishes messages to subscribers
"""
import logging

import six
import zmq

from . import constants
from .utils.devices import generate_device_name

logger = logging.getLogger(__name__)


class Publisher():
    """
        name (str): Name of this socket
        zcontext (:class:`zmq.Context`): socket context
        zsocket (:class:`zmq.Socket`):
    """

    def __init__(self, *args, **kwargs):
        self.zcontext = kwargs.get('context', zmq.Context.instance())
        self.name = six.ensure_binary(kwargs.get('name',
                                                 generate_device_name()))

        self.zsocket = kwargs.get('socket', self.zcontext.socket(zmq.PUB))
        self.zsocket.setsockopt(zmq.IDENTITY, self.name)

        self.status = constants.STATUS.ready

        return

    def listen(self, addr=None):
        """
        listen to address defined by `addr`

        Args:
            addr (str): Address to listen to as a connection string

        Raises:
            :class:`Exception`
        """
        if self.ready:
            self.zsocket.bind(addr)
            self.status = constants.STATUS.connected
            logger.debug('Publisher %s: Listen to %s' % (self.name, addr))
        else:
            raise Exception('Receiver %s not ready. status=%s' %
                            (self.name, self.status))

    def publish(self, topic, msg):
        logger.debug("Notifying topic: {}".format(topic))
        return self.zsocket.send_multipart([six.ensure_binary(topic),
                                            six.ensure_binary(msg)])

    @property
    def ready(self):
        """
        Property used to check if this receiver is ready.

        Returns:
            bool: True if the receiver is ready to connect or listen, otherwise
                False
        """
        return self.status == constants.STATUS.ready
