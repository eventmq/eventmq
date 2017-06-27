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
:mod:`pub` -- Publisher Node
============================
Publishes messages to subscribers
"""
import logging

from . import __version__, exceptions, poller, publisher, receiver
from .constants import STATUS
from .settings import conf, reload_settings
from .utils.classes import HeartbeatMixin

logger = logging.getLogger(__name__)


class Pub(HeartbeatMixin):
    def __init__(self, override_settings=None, skip_signal=False, *args,
                 **kwargs):
        """
        Initalize the publisher. Loads settings, creates sockets, loads them
        into a poller and prepares the publisher for a ``start()`` call.

        Args:
           override_settings (dict): Dictionary containing settings that will
               override the defaults and anything loaded from a config file.
               The key should match the upper case conf setting name. See
               :func:`eventmq.settings.load_settings_from_dict`
           skip_signal (bool): Don't register the signal handclers. Useful for
               testing.
        """
        self.override_settings = override_settings
        reload_settings('publisher', self.override_settings)

        logger.info('Initiaizing publisher...')
        logger.info('Publisher version: ' + __version__)

        super(Pub, self).__init__(*args, **kwargs)  # creates _meta
        self.poller = poller.Poller()
        self.incoming = receiver.Receiver()
        self.outgoing = publisher.Publisher()

        self.received_disconnect = False

        self.poller.register(self.incoming, poller.POLLIN)
        return

    def start(self):
        frontend_addr = conf.FRONTEND_LISTEN_ADDR
        backend_addr = conf.BACKEND_LISTEN_ADDR

        undefined_addresses = []

        if not frontend_addr:
            undefined_addresses.append('FRONTEND_LISTEN_ADDR')
        if not backend_addr:
            undefined_addresses.append('BACKEND_LISTEN_ADDR')

        if undefined_addresses:
            raise exceptions.ConnectionError(
                '{} must be defined before starting'.format(
                    ', '.join(undefined_addresses)))

        self.status = STATUS.starting

        self.incoming.listen(frontend_addr)
        self.outgoing.listen(backend_addr)

        logger.info('Listening for publish requests on {}'.format(
            frontend_addr))
        logger.info('Listening for subscribers on {}'.format(backend_addr))

        self._start_event_loop()

    def _start_event_loop(self):

        while True:
            if self.received_disconnect:
                break

            events = self.poller.poll()

            if events.get(self.incoming) == poller.POLLIN:
                msg = self.incoming.recv_multipart()
                self.process_client_message(msg)

    def process_client_message(self, msg):

        logger.debug(msg)

        command = msg[3]

        if command == 'PUBLISH':
            logger.debug('Got Publish command')
            topic = msg[5]
            sub_message = msg[6]
            logger.debug(self.outgoing.publish(topic, sub_message))

        return


# Entry point for pip console scripts
def pub_main():
    r = Pub()
    r.pub_main()
