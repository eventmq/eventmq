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
:mod:`classes` -- Utility Classes
=================================
Defines some classes to use when implementing ZMQ devices
"""
from collections import deque
import json
import logging
import sys

import six
import zmq.error

from .. import conf, constants, exceptions, poller, utils
from ..utils.encoding import encodify
from ..utils.messages import send_emqp_message as sendmsg
from ..utils.timeutils import monotonic, timestamp

logger = logging.getLogger(__name__)


class EMQPService(object):
    """
    Helper for devices that connect to brokers.

    Implements utility methods for sending EMQP messages for the following
    EMQP commands.
     - INFORM

    Also implements utlitiy methods for managing long-running processes.

    To use you must define:
     - `self.outgoing` - socket where messages can be sent to the Router
     - `self.SERVICE_TYPE` - defines the service type for INFORM. See
       :meth:`send_inform` for more information.
     - `self.poller` - the poller that `self.outgoing` will be using.
       Usually: `self.poller = eventmq.poller.Poller()`

    When messages are received from the router, they are processed in
    :meth:`process_message` which then calls `on_COMMAND`. This should be used
    in the event loop so if you want to respond to the SCHEDULE command, you
    would define the method `on_schedule` in your service class.

    See the code for :class:`Scheduler` and :class:`JobManager` for examples.
    """
    def send_inform(self, queues=()):
        """
        Notify the router that this job manager is online and and ready for
        work. This includes a list of queues the router should forward messages
        for.

        Args:
            type_ (str): Either 'worker' or 'scheduler'
            queues (list):
                - For 'worker' type, the queues the worker is listening on and
                  their weights.

                  Example:
                    ([10, 'default'], [15, 'push_notifications'])
                - Ignored for 'scheduler' type

        Raises:
            ValueError: When `type_` does not match a specified type

        Returns:
            str: ID of the message

        .. note::

           Passing a single string for queues is supported for backward
           compatibility and not recommended for new apps.
        """
        valid_types = (constants.CLIENT_TYPE.worker,
                       constants.CLIENT_TYPE.scheduler)

        if self.SERVICE_TYPE not in valid_types:
            raise ValueError('{} not one of {}'.format(self.SERVICE_TYPE,
                                                       valid_types))

        if self.SERVICE_TYPE == constants.CLIENT_TYPE.scheduler:
            queues = ''

        if isinstance(queues, (list, tuple)) and queues:
            # queues shouldn't be empty. We don't want to send '[]' to the
            # router.
            queues = json.dumps(queues)
        else:
            queues = ''

        msgid = sendmsg(self.outgoing, 'INFORM', [
            queues,
            self.SERVICE_TYPE
        ])

        # If heartbeating is active, update the last heartbeat time
        if hasattr(self, '_meta') and 'last_sent_heartbeat' in self._meta:
            self._meta['last_sent_heartbeat'] = monotonic()

        return msgid

    def _setup(self):
        """
        Prepares the service to connect to a broker. Actions that must
        also run on a reset are here.
        """
        # Look for incoming events
        self.poller.register(self.outgoing, poller.POLLIN)
        self.awaiting_startup_ack = False
        self.received_disconnect = False
        self.should_reset = False

        self.status = constants.STATUS.ready

    def start(self, addr, queues=conf.DEFAULT_QUEUE_NAME):
        """
        Connect to `addr` and begin listening for job requests

        Args:
            addr (str): connection string to connect to
        """
        while not self.received_disconnect:
            self.status = constants.STATUS.connecting
            self.outgoing.connect(addr)

            # Setting this to false is how the loop is broken and the
            # _event_loop is started.
            self.awaiting_startup_ack = True

            # If this is inside the loop, then many inform messages will stack
            # up on the buffer until something is actually connected to.
            self.send_inform(queues)

            # We don't want to accidentally start processing jobs before our
            # connection has been setup completely and acknowledged.
            while self.awaiting_startup_ack:
                # Poller timeout is in ms so the reconnect timeout is
                # multiplied by 1000 to get seconds
                events = self.poller.poll(conf.RECONNECT_TIMEOUT * 1000)

                if self.outgoing in events:  # A message from the Router!
                    msg = self.outgoing.recv_multipart()
                    # TODO This will silently drop messages that aren't
                    # ACK/DISCONNECT
                    if msg[2] == "ACK" or msg[2] == "DISCONNECT":
                        # :meth:`on_ack` will set self.awaiting_startup_ack to
                        # False
                        self.process_message(msg)

            self.status = constants.STATUS.connected

            if not self.received_disconnect:
                logger.info('Starting event loop...')
                self._start_event_loop()

            # When we return, soemthing has gone wrong and try to reconnect
            # unless self.received_disconnect is True
            if not self.received_disconnect or self.should_reset:
                self.reset()

        logger.info('Death.')

    def reset(self):
        """
        Resets the current connection by closing and reopening the socket
        """
        # Unregister the old socket from the poller
        self.poller.unregister(self.outgoing)

        # Polish up a new socket to use
        self.outgoing.rebuild()

        # Prepare the device to connect again
        self._setup()

    def process_message(self, msg):
        """
        Processes a message. Processing takes form of calling an
        `on_EMQP_COMMAND` method. The method must accept `msgid` and `message`
        as the first arguments.

        Args:
            msg: The message received from the socket to parse and process.
        """
        if self.is_heartbeat_enabled:
            # Any received message should count as a heartbeat
            self._meta['last_received_heartbeat'] = monotonic()
            if self._meta['heartbeat_miss_count']:
                # Reset the miss count too
                self._meta['heartbeat_miss_count'] = 0

        try:
            message = utils.messages.parse_message(msg)
        except exceptions.InvalidMessageError:
            logger.exception('Invalid message: %s' % str(msg))
            return

        command = message[0].lower()
        msgid = message[1]
        message = message[2]

        if hasattr(self, "on_%s" % command):
            func = getattr(self, "on_%s" % command)
            func(msgid, message)
        else:
            logger.warning('No handler for %s found (tried: %s)' %
                           (command.upper(), ('on_%s' % command)))

    def on_ack(self, msgid, ackd_msgid):
        """
        Sets :attr:`awaiting_ack` to False
        """
        # The msgid is the only frame in the message
        ackd_msgid = ackd_msgid[0]
        logger.info('Received ACK for router (or client) %s' % ackd_msgid)
        self.awaiting_startup_ack = False

    def on_disconnect(self, msgid, msg):
        # To break out of the connecting loop if necessary
        self.awaiting_startup_ack = False

        # Loops event loops should check for this and break out
        self.received_disconnect = True

    @property
    def is_heartbeat_enabled(self):
        """
        Property to check if heartbeating is enabled. Useful when certain
        properties must be updated for heartbeating
        Returns:
            bool - True if heartbeating is enabled, False if it isn't
        """
        if hasattr(self, '_meta') and 'last_sent_heartbeat' in self._meta:
            return True
        return False


class HeartbeatMixin(object):
    """
    Provides methods for implementing heartbeats
    """
    def __init__(self, *args, **kwargs):
        """
        Sets up some variables to track the state of heartbeaty things
        """
        super(HeartbeatMixin, self).__init__()
        if not hasattr(self, '_meta'):
            self._meta = {}

        self.reset_heartbeat_counters()

    def reset_heartbeat_counters(self):
        """
        Resets all the counters for heartbeats back to 0
        """
        # the monotonic clock is used for the interval values like
        # 'last_sent_heartbeat'
        self._meta['last_sent_heartbeat'] = 0
        self._meta['last_received_heartbeat'] = 0
        self._meta['heartbeat_miss_count'] = 0

    def send_heartbeat(self, socket):
        """
        Send a HEARTBEAT command to the specified socket

        Args:
            socket (socket): The eMQP socket to send the message to

        Return:
            str: ID of the message
        """
        # Note: When updating this function, also make sure the custom versions
        # acts as expected in router.py
        msgid = sendmsg(socket, 'HEARTBEAT', str(timestamp()))
        self._meta['last_sent_heartbeat'] = monotonic()

        return msgid

    def is_dead(self, now=None):
        """
        Checks the heartbeat counters to find out if the thresholds have been
        met.

        Args:
            now (float): The time to use to check if death has occurred. If
                this value is None, then :func:`utils.timeutils.monotonic`
                is used.

        Returns:
            bool: True if the connection to the peer has died, otherwise
                False
        """
        if not now:
            now = monotonic()

        if now - self._meta['last_received_heartbeat'] >= \
           conf.HEARTBEAT_TIMEOUT:
            self._meta['heartbeat_miss_count'] += 1
            self._meta['last_received_heartbeat'] = now

            if self._meta['heartbeat_miss_count'] >= \
               conf.HEARTBEAT_LIVENESS:
                return True

        return False

    def maybe_send_heartbeat(self, events):
        # TODO: Optimization: Move the method calls into another thread so
        # they don't block the event loop
        if not conf.DISABLE_HEARTBEATS:
            now = monotonic()
            # Send a HEARTBEAT if necessary
            if now - self._meta['last_sent_heartbeat'] >= \
               conf.HEARTBEAT_INTERVAL:
                self.send_heartbeat(self.outgoing)

            # Do something about any missed HEARTBEAT, if we have nothing
            # waiting on the socket
            if self.is_dead() and not events:
                logger.critical(
                    'The broker appears to have gone away. '
                    'Reconnecting...')
                return False
            return True


class ZMQReceiveMixin(object):
    """
    Defines some methods for receiving messages. This class will not work if
    used on it's own
    """
    def recv(self):
        """
        Receive a message
        """
        if sys.version[0] == '2':
            msg = self.zsocket.recv()
        else:
            msg = self.zsocket.recv_string()

        if not ("HEARTBEAT" == msg[2] or "HEARTBEAT" == msg[3]) or \
                not conf.HIDE_HEARTBEAT_LOGS:
            logger.debug('Received message: {}'.format(msg))
        return msg

    def recv_multipart(self):
        """
        Receive a multipart message
        """
        msg = self.zsocket.recv_multipart()

        # Decode bytes to strings in python3
        if sys.version[0] == '3' and type(msg[0] in (bytes,)):
            msg = [m.decode() for m in msg]

        # If it's not at least 4 frames long then most likely it isn't an
        # eventmq message
        if len(msg) >= 4 and \
            not ("HEARTBEAT" == msg[2] or "HEARTBEAT" == msg[3]) or \
                not conf.HIDE_HEARTBEAT_LOGS:
            logger.debug('Received message: {}'.format(msg))
        return msg


class ZMQSendMixin(object):
    """
    Defines some methods for sending messages. This class will not work if used
    on it's own
    """
    def send_multipart(self, message, protocol_version, _recipient_id=None):
        """
        Send a message directly to the 0mq socket. Automatically inserts some
        frames for your convience. The sent frame ends up looking something
        like this

            (_recipient_id, '', protocol_version) + (your, tuple)

        Args:
            message (tuple): Raw message to send.
            protocol_version (str): protocol version. it's good practice but
                you may explicitly specify None to skip adding the version
            _recipient_id (object): When using a :attr:`zmq.ROUTER` you must
                specify the the recipient id of the remote socket
        """
        supported_msg_types = (tuple, list)
        if not isinstance(message, supported_msg_types):
            raise exceptions.MessageError(
                '%s message type not one of %s' %
                (type(message), str(supported_msg_types)))

        if isinstance(message, list):
            message = tuple(message)

        if _recipient_id:
            headers = (_recipient_id, '', protocol_version)
        else:
            headers = ('', protocol_version, )

        msg = encodify(headers + message)

        # Decode bytes to strings in python3
        if sys.version[0] == '3' and type(msg[0] in (bytes,)):
            msg = [m.decode() for m in msg]

        # If it's not at least 4 frames long then most likely it isn't an
        # eventmq message
        if len(msg) > 4 and \
            not ("HEARTBEAT" == msg[2] or "HEARTBEAT" == msg[3]) or \
                not conf.HIDE_HEARTBEAT_LOGS:
            logger.debug('Sending message: %s' % str(msg))

        try:
            self.zsocket.send_multipart([six.ensure_binary(m) for m in msg],
                                        flags=zmq.NOBLOCK)
        except zmq.error.ZMQError as e:
            if 'No route' in str(e):
                raise exceptions.PeerGoneAwayError(e)

    def send(self, message, protocol_version):
        """
        Sends a message

        Args:
            message: message to send to something
            protocol_version (str): protocol version. it's good practice, but
                you may explicitly specify None to skip adding the version
        """
        self.send_multipart((message, ), protocol_version)


class EMQdeque(object):
    """
    EventMQ deque based on python's collections.deque with full and
    programmable full.

    .. note::

       Because of the programmable full, some of the methods that would
       normally return None return a boolean value that should be captured and
       checked to ensure proper error handling.

    """
    def __init__(self, full=None, pfull=None, on_full=None, initial=()):
        """
        Args:
            full (int): Hard limit on deque size. Rejects adding elements.
                Default: 0 - no limit
            pfull (int): Programmable limit on deque size, defaults
                 to ``full`` length
            on_full (func): callback to call when ``full`` limit is hit
            initial (iter): The initial iteratable used to contruct the deque
        """
        self.full = full if not None else 0
        self.pfull = pfull if not None else full

        self._queue = deque(initial, maxlen=self.full)
        self.on_full = on_full

    def __str__(self):
        return "{}".format(str(self._queue))

    def __unicode__(self):
        return "{}".format(six.text_type(self._queue))

    def __repr__(self):
        return "{}".format(repr(self._queue))

    def __iter__(self):
        return self._queue.__iter__()

    def __len__(self):
        return len(self._queue)

    def append(self, item):
        """
        Append item to the right this deque if the deque isn't full.

        .. note::

            You should check the return value of this call and handle the cases
            where False is returned.

        Returns:
            bool: True if ``item`` was successfully added, False if the deque
                is at the ``self.full`` limit. If it is, ``self.on_full`` is
                called.
        """
        if self.is_full():
            if self.on_full:
                self.on_full()
            return False
        else:
            self._queue.append(item)
            return True

    def remove(self, item):
        """
        Remove ``item`` from the deque.

        Args:
           item (object): The item to remove from the deque
        """
        return self._queue.remove(item)

    def is_full(self):
        """
        Check to see if the deque contains ``self.full`` items.

        Returns:
            bool: True if the deque contains at least ``full`` items. False
            otherwise
        """
        if self.full and self.full != 0:
            return len(self._queue) >= self.full
        else:
            return False

    def is_empty(self):
        """
        Check to see if the deque contains no items.

        Returns:
            bool: True if the deque contains 0 items. False otherwise
        """
        return len(self._queue) == 0

    def is_pfull(self):
        """
        Check to see if the deque contains ``self.pfull`` items.

        Returns:
            bool: True if the deque contains at least ``pfull`` items.
            False otherwise
        """
        if self.pfull and self.pfull != 0:
            return len(self._queue) >= self.pfull
        else:
            return False

    def pop(self):
        """
        Returns:
            object: the last (right-most) element of the deque
        """
        return self._queue.pop()

    def popleft(self):
        """
        Returns:
            object: the first (left-most) element of the deque
        """
        return self._queue.popleft()

    def peek(self):
        """
        Returns:
            object: the last (right-most) element of the deque
        """
        return self._queue[-1]

    def peekleft(self):
        """
        Returns:
            object: the first (left-most) element of the deque
        """
        return self._queue[0]

    def appendleft(self, item):
        """
        Append item to the left this deque if the deque isn't full.

        .. note::

            You should check the return value of this call and handle the cases
            where False is returned.

        Returns:
            bool: True if ``item`` was successfully added, False if the deque
                is at the ``self.full`` limit. If it is, ``self.on_full`` is
                called.
        """
        if self.is_full():
            if self.on_full:
                self.on_full()
            return False
        else:
            self._queue.appendleft(item)
            return True

    def extend(self, iterable):
        """
        append ``iterable`` to the right (end) of the deque

        Returns:
            bool: True if ``item`` was successfully added, False if the deque
                is at the ``self.full`` limit. If it is, ``self.on_full`` is
                called.
        """
        if self.full and self.full > 0 and \
           len(self._queue) + len(iterable) >= self.full:

            if len(self._queue) >= self.full and self.on_full:
                self.on_full()
            return False
        else:
            self._queue.extend(iterable)
