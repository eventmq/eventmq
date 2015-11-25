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
:mod:`router` -- Router
=======================
Routes messages to workers (that are in named queues).
"""
import uuid

from . import conf, exceptions, log, poller, receiver
from .constants import STATUS
from .utils.classes import HeartbeatMixin
from .utils.messages import (
    send_emqp_router_message as sendmsg,
    parse_router_message
)
from .utils.timeutils import monotonic, timestamp


logger = log.get_logger(__file__)


class Router(HeartbeatMixin):
    """
    A simple router of messages

    This router uses tornado's eventloop.
    """

    def __init__(self, *args, **kwargs):
        super(Router, self).__init__(*args, **kwargs)

        self.name = str(uuid.uuid4())
        logger.info('Initializing Router %s...' % self.name)

        self.poller = poller.Poller()

        self.incoming = receiver.Receiver()
        self.outgoing = receiver.Receiver()

        self.poller.register(self.incoming, poller.POLLIN)
        self.poller.register(self.outgoing, poller.POLLIN)

        self.status = STATUS.ready

        # List of workers by queue name
        self.queues = {}
        # List of queues by workers
        self.workers = {}

    def start(self,
              frontend_addr='tcp://127.0.0.1:47290',
              backend_addr='tcp://127.0.0.1:47291'):
        """
        Begin listening for connections on the provided connection strings

        Args:
            frontend_addr (str): connection string to listen for requests
            backend_addr (str): connection string to listen for workers
        """
        self.status = STATUS.starting

        self.incoming.listen(frontend_addr)
        self.outgoing.listen(backend_addr)

        self.status = STATUS.listening
        logger.info('Listening for requests on %s' % frontend_addr)
        logger.info('Listening for workers on %s' % backend_addr)

        self._start_event_loop()

    def _start_event_loop(self):
        """
        Starts the actual eventloop. Usually called by :meth:`Router.start`
        """
        while True:
            now = monotonic()
            events = self.poller.poll()

            if events.get(self.incoming) == poller.POLLIN:
                msg = self.incoming.recv_multipart()
                self.on_receive_request(msg)

            if events.get(self.outgoing) == poller.POLLIN:
                msg = self.outgoing.recv_multipart()
                self.on_receive_reply(msg)

            if not conf.DISABLE_HEARTBEATS:
                # Send a HEARTBEAT if necessary
                if now - self._meta['last_sent_heartbeat'] >= \
                   conf.HEARTBEAT_INTERVAL:
                    self.send_workers_heartbeats()
                    self._meta['last_sent_heartbeat'] = monotonic()

    def send_ack(self, socket, recipient, msgid):
        """
        Sends an ACK response
        """
        logger.info('Sending ACK to %s' % recipient)
        sendmsg(socket, recipient, 'ACK', msgid)

    def send_heartbeat(self, socket, recipient):
        """
        Custom send heartbeat method to take into account the recipient that is
        needed when building messages
        """
        sendmsg(socket, recipient, 'HEARTBEAT', str(timestamp()))

    def send_workers_heartbeats(self):
        """
        Send heartbeats to the registered workers.
        """
        for k in self.workers:
            self.send_heartbeat(self.outgoing, k)

    def on_heartbeat(self, sender, msgid, msg):
        """
        """

    def on_inform(self, sender, msgid, msg):
        """
        Handles an INFORM message. Usually when new worker coming online
        """
        logger.info('Received INFORM request from %s' % sender)
        queue_name = msg[0]

        self.workers[sender] = queue_name
        if queue_name in self.queues:
            self.queues[queue_name] += (sender,)
        else:
            self.queues[queue_name] = (sender,)

        self.send_ack(self.outgoing, sender, msgid)

    def on_receive_request(self, msg):
        """
        This function is called when a message comes in from the client socket.
        It then calls `on_command`. If `on_command` isn't found, then a
        warning is created.
        """
        try:
            message = parse_router_message(msg)
        except exceptions.InvalidMessageError:
            logger.exception('Invalid message from clients: %s' % str(msg))

        queue_name = message[3][0]

        # do some things and forward it to the workers
        self.outgoing.send_multipart(msg)

        # If we have no workers for the queue TODO something about it
        if queue_name not in self.queues:
            logger.warning("Received REQUEST with a queue I don't recognize")

    def on_receive_reply(self, msg):
        """
        This method is called when a message comes in from the worker socket.
        It then calls `on_command`. If `on_command` isn't found, then a warning
        is created.

        def on_inform(msg):
            pass
        """
        try:
            message = parse_router_message(msg)
        except exceptions.InvalidMessageError:
            logger.exception('Invalid message from workers: %s' % str(msg))
            return

        sender = message[0]
        command = message[1]
        msgid = message[2]
        message = message[3]

        if hasattr(self, "on_%s" % command.lower()):
            func = getattr(self, "on_%s" % command.lower())
            func(sender, msgid, message)
