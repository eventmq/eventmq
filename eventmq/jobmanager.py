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
:mod:`jobmanager` -- Job Manager
================================
Ensures things about jobs and spawns the actual tasks
"""
import uuid

from . import conf, constants, exceptions, log, utils
from .poller import Poller, POLLIN
from .sender import Sender
from .utils.messages import send_emqp_message as sendmsg
import utils.messages
from .utils.timeutils import monotonic, timestamp

logger = log.get_logger(__file__)


class JobManager(object):
    """
    The exposed portion of the worker. The job manager's main responsibility is
    to manage the resources on the server it's running.

    This job manager uses tornado's eventloop.
    """

    def __init__(self, *args, **kwargs):
        """
        .. note ::
           All args are optional unless otherwise noted.

        Args:
            name (str): unique name of this instance. By default a uuid will be
                 generated.
        """
        self.name = kwargs.get('name', str(uuid.uuid4()))
        self.incoming = Sender()
        self.poller = Poller()

        self._setup()

    def _setup(self):
        """
        Gets JobManager ready to connect to a broker
        """
        # Look for incoming events
        self.poller.register(self.incoming, POLLIN)
        self.awaiting_startup_ack = False

        # Meta data such as times, and counters are stored here
        self._meta = {
            'last_sent_heartbeat': 0,
            'last_received_heartbeat': 0,
            'heartbeat_miss_count': 0,
        }

        self.status = constants.STATUS.ready

    def start(self, addr='tcp://127.0.0.1:47291'):
        """
        Connect to `addr` and begin listening for job requests

        Args:
            addr (str): connection string to connect to
        """
        self.status = constants.STATUS.connecting
        self.incoming.connect(addr)

        self.awaiting_startup_ack = True

        while self.awaiting_startup_ack:
            self.send_inform()
            # Poller timeout is in ms so we multiply it to get seconds
            events = self.poller.poll(conf.RECONNECT_TIMEOUT * 1000)
            if self.incoming in events:
                msg = self.incoming.recv_multipart()
                # We don't want to accidentally start processing jobs before
                # our conenction has been setup completely and acknowledged.
                if msg[2] != "ACK":
                    continue
                self.process_message(msg)

        if not self.awaiting_startup_ack:
            logger.info('Starting to listen for jobs')
            self._start_event_loop()

    def restart(self):
        """
        Restarts the current connection by closing and reopening the socket
        """
        # Unregister the old socket from the poller
        self.poller.unregister(self.incoming)

        # Polish up a new socket to use
        self.incoming.rebuild()

        # Prepare the device to connect again
        self._setup()

        self.start()

    def _start_event_loop(self):
        """
        Starts the actual eventloop. Usually called by :meth:`JobManager.start`
        """
        self.status = constants.STATUS.connected

        while True:
            now = monotonic()
            events = self.poller.poll()

            if events.get(self.incoming) == POLLIN:
                msg = self.incoming.recv_multipart()
                self.process_message(msg)

            if not conf.DISABLE_HEARTBEATS:
                # Send a HEARTBEAT if necessary
                if now - self._meta['last_sent_heartbeat'] >= \
                conf.HEARTBEAT_INTERVAL:
                    if conf.SUPER_DEBUG:
                        logger.debug(now - self._meta['last_sent_heartbeat'])
                    self.send_heartbeat()

                # Do something about any missed HEARTBEAT
                if now - self._meta['last_received_heartbeat'] >= \
                conf.HEARTBEAT_TIMEOUT:
                    # Update as if we got the last heartbeat so we can check in
                    # interval again
                    self._meta['heartbeat_miss_count'] += 1
                    self._meta['last_received_heartbeat'] = monotonic()
                    if self._meta['heartbeat_miss_count'] >= \
                    conf.HEARTBEAT_LIVENESS:
                        logger.critical(
                            'The broker appears to have gone away. '
                            'Reconnecting...')
                        break
        self.restart()

    def process_message(self, msg):
        """
        Processes a message

        Args:
            msg: The message received from the socket to parse and process.
                Processing takes form of calling an `on_COMMAND` method.
        """
        # Any received message should count as a heartbeat
        self._meta['last_received_heartbeat'] = monotonic()
        if self._meta['heartbeat_miss_count']:
            self._meta['heartbeat_miss_count'] = 0  # Reset the miss count too

        try:
            message = utils.messages.parse_message(msg)
        except exceptions.InvalidMessageError:
            logger.error('Invalid message: %s' % str(msg))
            return

        if conf.SUPER_DEBUG:
            logger.debug("Received Message: %s" % msg)

        command = message[0]
        msgid = message[1]
        message = message[2]

        if hasattr(self, "on_%s" % command.lower()):
            logger.debug('Calling on_%s' % command.lower())
            func = getattr(self, "on_%s" % command.lower())
            func(msgid, message)
        else:
            logger.warning('No handler for %s found (tried: %s)' %
                           (command, ('on_%s' % command.lower)))

    def send_inform(self):
        """
        Send an INFORM command
        """
        sendmsg(self.incoming, 'INFORM', 'default_queuename')

    def send_heartbeat(self):
        """
        Send a HEARTBEAT command to the connected broker
        """
        sendmsg(self.incoming, 'HEARTBEAT', str(timestamp()))
        self._meta['last_sent_heartbeat'] = monotonic()

    def on_ack(self, msgid, ackd_msgid):
        """
        Sets :attr:`awaiting_ack` to False
        """
        # The msgid is the only frame in the message
        ackd_msgid = ackd_msgid[0]
        logger.info('Received ACK for %s' % ackd_msgid)
        self.awaiting_startup_ack = False
