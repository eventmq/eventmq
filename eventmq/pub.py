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

from eventmq.log import setup_logger

from . import conf, poller, publisher, receiver
from .constants import STATUS
from .utils.classes import HeartbeatMixin
from .utils.settings import import_settings

logger = logging.getLogger(__name__)


class Pub(HeartbeatMixin):
    def __init__(self):
        self.poller = poller.Poller()
        self.frontend = receiver.Receiver()
        self.backend = publisher.Publisher()

        self.received_disconnect = False

        self.poller.register(self.frontend, poller.POLLIN)
        return

    def start(self,
              frontend_addr=conf.FRONTEND_LISTEN_ADDR,
              backend_addr=conf.BACKEND_LISTEN_ADDR):

        self.status = STATUS.starting

        self.frontend.listen(frontend_addr)
        self.backend.listen(backend_addr)

        logger.info('Listening for publish requests on {}'.format(
            frontend_addr))
        logger.info('Listening for subscribers on {}'.format(backend_addr))

        self._start_event_loop()

    def _start_event_loop(self):

        while True:
            if self.received_disconnect:
                break

            events = self.poller.poll()

            if events.get(self.frontend) == poller.POLLIN:
                msg = self.frontend.recv_multipart()
                self.process_client_message(msg)

    def process_client_message(self, msg):

        logger.debug(msg)

        command = msg[3]

        if command == 'PUBLISH':
            logger.debug('Got Publish command')
            topic = msg[5]
            sub_message = msg[6]
            logger.debug(self.backend.publish(topic, sub_message))

        return

    def pub_main(self):
        """
        Kick off PubSub with logging and settings import
        """
        setup_logger('eventmq')
        import_settings(section='publisher')
        self.start(frontend_addr=conf.FRONTEND_LISTEN_ADDR,
                   backend_addr=conf.BACKEND_LISTEN_ADDR)


# Entry point for pip console scripts
def pub_main():
    r = Pub()
    r.pub_main()
