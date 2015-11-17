"""
:mod:`router` -- Router
=======================
Routes messages to workers (that are in named queues).
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

from zmq.eventloop import ioloop
from eventmq import STATUS

import log
import receiver
import sender

logger = log.get_logger(__file__)


class Router(object):
    """
    A simple router of messages
    """

    def __init__(self, *args, **kwargs):
        logger.info('Initializing Router...')
        ioloop.install()
        self.name = str(uuid.uuid4())

        self.incoming = receiver.Receiver(callable=self.on_receive_request,
                                          skip_zmqstream=False)
        self.outgoing = sender.Sender(skip_zmqstream=False)

        self.status = STATUS.ready
        logger.info('Done initializing Router')

    def start(self,
              frontend_addr='tcp://127.0.0.1:47290',
              backend_addr='tcp://127.0.0.1:47291'):
        """
        Begin listening for connections on the provided connection strings

        :param frontend_addr: connection string to listen for requests
        :type incoming: str
        :param backend_addr: connection string to listen for workers
        :type outgoing: str
        """
        self.status = STATUS.starting

        self.incoming.listen(frontend_addr)
        # self.outgoing.listen(backend_addr)

        self.status = STATUS.listening
        logger.info('Listening for requests on %s' % frontend_addr)
        logger.info('Listening for workers on %s' % backend_addr)

        ioloop.IOLoop.instance().start()

    def on_receive_request(self, msg):
        logger.debug(msg)
        self.outgoing.send_raw(msg)

if __name__ == "__main__":
    r = Router()
    r.start()
