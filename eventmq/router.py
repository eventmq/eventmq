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
from copy import copy
import logging

from . import conf, exceptions, poller, receiver
from .constants import STATUS
from .utils.classes import HeartbeatMixin
from .utils.messages import (
    send_emqp_router_message as sendmsg,
    fwd_emqp_router_message as fwdmsg,
    parse_router_message
)
from .utils.devices import generate_device_name
from .utils.timeutils import monotonic, timestamp


logger = logging.getLogger(__name__)


class Router(HeartbeatMixin):
    """
    A simple router of messages
    """
    def __init__(self, *args, **kwargs):
        super(Router, self).__init__(*args, **kwargs)  # Creates _meta

        self.name = generate_device_name()
        logger.info('Initializing Router %s...' % self.name)

        self.poller = poller.Poller()

        self.incoming = receiver.Receiver()
        self.outgoing = receiver.Receiver()

        self.poller.register(self.incoming, poller.POLLIN)
        self.poller.register(self.outgoing, poller.POLLIN)

        self.status = STATUS.ready

        # Tracks the last time the worker queues were cleaned
        self._meta['last_worker_cleanup'] = 0

        # Worker queues by queue name. The lists here are Last Recently Used
        # queues where a worker is popped off when given a job, and appeneded
        # when one finishes
        self.queues = {}

        # List of queues by workers. This helps to clear out old workers from
        # self.queues.
        # Keys:
        #    queues: list() of queues the worker belongs to
        #    hb: monotonic timestamp of the last received message from worker
        self.workers = {}

        # Message buffer. When messages can't be sent because there are no
        # workers available to take the job
        self.waiting_messages = {}

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
                self.process_worker_message(msg)

            if not conf.DISABLE_HEARTBEATS:
                # Send a HEARTBEAT if necessary
                if now - self._meta['last_sent_heartbeat'] >= \
                   conf.HEARTBEAT_INTERVAL:
                    self.send_workers_heartbeats()

                if now - self._meta['last_worker_cleanup'] >= 10:
                    # Loop through the next worker queue and clean up any dead
                    # ones so the next one is alive
                    self.clean_up_dead_workers()

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

        Args:
            socket (socket): the socket to send the heartbeat with
            recipient (str): Worker I
        """
        sendmsg(socket, recipient, 'HEARTBEAT', str(timestamp()))

    def send_workers_heartbeats(self):
        """
        Send heartbeats to the registered workers.
        """
        self._meta['last_sent_heartbeat'] = monotonic()

        for worker_id in self.workers:
            self.send_heartbeat(self.outgoing, worker_id)

    def on_heartbeat(self, sender, msgid, msg):
        """
        a placeholder for a noop command. The actual 'logic' for HEARTBEAT is
        in :meth:`self.process_worker_message` because any message from a
        worker counts as a HEARTBEAT
        """

    def on_inform(self, sender, msgid, msg):
        """
        Handles an INFORM message. Usually when new worker coming online
        """
        logger.info('Received INFORM request from %s' % sender)
        queue_name = msg[0]

        self.add_worker(sender, queue_name)

        self.send_ack(self.outgoing, sender, msgid)

    def on_ready(self, sender, msgid, msg):
        """
        A worker that we should already know about is ready for another job
        """
        queue_name = None  # stores the queue name
        # if there are waiting messages for the queues this worker is a member
        # of, then reply back with the oldest waiting message, otherwise just
        # add the worker to the list of available workers.
        if self.workers[sender]['queues'] in self.waiting_messages:
            queue_name = self.workers[sender]['queues']

            logger.debug('Found waiting message in the %s waiting messages '
                         'queue' % queue_name)
            msg = self.waiting_messages[queue_name].pop()
            fwdmsg(self.outgoing, sender, msg[1:])  # strip off client id.

            # It is easier to check if a key exists rather than the len of a
            # key if it exists, so if that was the last message remove the
            # queue
            if len(self.waiting_messages[queue_name]) is 0:
                logger.debug('No more messages in waiting_messages queue %s. '
                             'Removing...' % queue_name)
                del self.waiting_messages[queue_name]
        else:
                self.requeue_worker(sender)

    def clean_up_dead_workers(self):
        """
        Loops through the worker queues and removes any workers who haven't
        responded in HEARTBEAT_TIMEOUT
        """
        now = monotonic()
        self._meta['last_worker_cleanup'] = now

        # Because workers are removed from inside the loop, a copy is needed to
        # prevent the dict we are iterating over from changing.
        workers = copy(self.workers)
        for worker_id in workers:

            # If a worker started, then immediatly died then no hb dictionary
            # was created so we should just remove that worker.
            # hb stands for heartbeat
            if 'hb' not in self.workers[worker_id]:
                logger.info('Removing worker %s from the queue due to no '
                            'heartbeat' % (worker_id))
                del self.workers[worker_id]
                continue

            last_hb_seconds = now - self.workers[worker_id]['hb']
            if last_hb_seconds >= conf.HEARTBEAT_TIMEOUT:
                logger.info("No messages from worker %s in %s. Removing from "
                            "the queue" % (worker_id, last_hb_seconds))
                # Remove the worker from the actual worker queues
                del self.workers[worker_id]

    def add_worker(self, worker_id, queues=None):
        """
        Adds a worker to worker queues

        Args:
            worker_id: unique id of the worker to add
            queues: queue or queues this worker should be a member of
        """
        # Add the worker to our worker dict
        self.workers[worker_id] = {}
        self.workers[worker_id]['queues'] = queues

        # Add the worker to the queues it supports
        if queues in self.queues:
            self.queues[queues] += [worker_id, ]
        else:
            self.queues[queues] = [worker_id, ]

        logger.debug('Adding %s to the worker pool for %s' %
                     (worker_id, str(queues)))

    def requeue_worker(self, worker_id):
        """
        Add a worker back to the queue pool
        """
        if worker_id in self.workers:
            queues = self.workers[worker_id].get('queues', None)
        else:
            queues = None

        if queues:
            logger.debug('Readding worker {} to queues {}'.
                         format(worker_id, queues))

            self.queues[queues].append(worker_id)

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

        # If we have no workers for the queue TODO something about it
        if queue_name not in self.queues:
            logger.warning("Received REQUEST with a queue I don't recognize")
        print self.queues
        try:
            worker_addr = self.queues[queue_name].pop()
        except KeyError:
            logger.critical("REQUEST for an unknown queue")
            return
        except IndexError:
            logger.warning('No available workers for queue "%s". Buffering '
                           'message to send later.' % queue_name)
            if queue_name not in self.waiting_messages:
                self.waiting_messages[queue_name] = []
            self.waiting_messages[queue_name].append(msg)
            logger.debug('%d waiting messages in queue "%s"' %
                         (len(self.waiting_messages[queue_name]), queue_name))
            return

        fwdmsg(self.outgoing, worker_addr, msg[1:])  # strip off the client id

    def process_worker_message(self, msg):
        """
        This method is called when a message comes in from the worker socket.
        It then calls `on_COMMAND.lower()`. If `on_command` isn't found, then
        a warning is created.

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

        # Treat any message like a HEARTBEAT.
        if sender in self.workers:
            self.workers[sender]['hb'] = monotonic()
        elif command.lower() != 'inform':
            logger.critical('Unknown worker %s attempting to run %s command: '
                            '%s' % (sender, command, str(msg)))
            return

        if hasattr(self, "on_%s" % command.lower()):
            func = getattr(self, "on_%s" % command.lower())
            func(sender, msgid, message)