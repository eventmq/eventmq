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
:mod:`router` -- Router
=======================
Routes messages to workers (that are in named queues).
"""
from copy import copy
import json  # deserialize queues in on_inform. should be refactored
import logging
import signal

from six.moves import map

from eventmq.log import setup_logger, setup_wal_logger
from . import __version__
from . import conf, constants, exceptions, poller, receiver
from .constants import (
    CLIENT_TYPE, DISCONNECT, KBYE, PROTOCOL_VERSION, ROUTER_SHOW_SCHEDULERS,
    ROUTER_SHOW_WORKERS, STATUS
)
from .utils import tuplify
from .utils.classes import EMQdeque, HeartbeatMixin
from .utils.devices import generate_device_name
from .utils.messages import (
    fwd_emqp_router_message as fwdmsg,
    parse_router_message,
    send_emqp_router_message as sendmsg,
)
from .utils.settings import import_settings
from .utils.timeutils import monotonic, timestamp


logger = logging.getLogger(__name__)
wal_logger = logging.getLogger('eventmq-wal')


class Router(HeartbeatMixin):
    """
    A simple router of messages
    """

    def __init__(self, *args, **kwargs):
        super(Router, self).__init__(*args, **kwargs)  # Creates _meta

        setup_logger("eventmq")

        self.name = generate_device_name()
        logger.info('EventMQ Version {}'.format(__version__))
        logger.info('Initializing Router {}...'.format(self.name))

        self.poller = poller.Poller()

        self.incoming = receiver.Receiver()
        self.outgoing = receiver.Receiver()
        self.administrative_socket = receiver.Receiver()

        self.poller.register(self.incoming, poller.POLLIN)
        self.poller.register(self.outgoing, poller.POLLIN)
        self.poller.register(self.administrative_socket, poller.POLLIN)

        self.status = STATUS.ready

        #: Tracks the last time the worker queues were cleaned of dead workers
        self._meta['last_worker_cleanup'] = 0

        #: JobManager address by queue name. The lists here are Last Recently
        #: Used queues where a worker is popped off when given a job, and
        #: appeneded when one finishes. There is one entry per available
        #: worker slot, so you may see duplicate addresses.
        #:
        #: Example:
        #:     {'default': ['w1', 'w2', 'w1', 'w4']}
        self.queues = {}

        #: List of queues by workers. Meta data about the worker such as the
        #: queue memebership and timestamp of last message received are stored
        #: here.
        #:
        #: **Keys**
        #:  * ``queues``: list() of queue names and prioritiess the worker
        #:    belongs to. e.g. (10, 'default')
        #:  * ``hb``: monotonic timestamp of the last received message from
        #:    worker
        #:  * ``available_slots``: int count of jobs this manager can still
        #:    process.
        self.workers = {}

        #: Message buffer. When messages can't be sent because there are no
        #: workers available to take the job
        self.waiting_messages = {}

        # Key: Queue.name, Value: # of messages sent to workers on that queue
        # Includes REQUESTS in flight but not REQUESTS queued
        self.processed_message_counts = {}

        # Same as above but Key: Worker.uuid
        self.processed_message_counts_by_worker = {}

        #: Tracks the last time the scheduler queue was cleaned out of dead
        #: schedulers
        self._meta['last_scheduler_cleanup'] = 0

        #: Queue for schedulers to use:
        self.scheduler_queue = []

        #: Scheduler clients. Clients are able to send SCHEDULE commands that
        #: need to be routed to a scheduler, which will keep track of time and
        #: run the job.
        #: Contains dictionaries:
        #:     self.schedulers[<scheduler_zmq_id>] = {
        #:       'hb': <last_recv_heartbeat>,
        #:     }
        self.schedulers = {}

        #: Latency tracking dictionary
        #: Key: msgid of msg each REQUEST received and forwarded to a worker
        #: Value: (timestamp, queue_name)
        self.job_latencies = {}

        #: Excecuted function tracking dictionary
        #: Key: msgid of msg each REQUEST received and forwarded to a worker
        #: Value: (function_name, queue_name)
        #: Set to True when the router should die.
        self.received_disconnect = False

        # Tests skip setting the signals.
        if not kwargs.pop('skip_signal', False):
            signal.signal(signal.SIGHUP, self.sighup_handler)
            signal.signal(signal.SIGUSR1, self.handle_pdb)

    def handle_pdb(self, sig, frame):
        import pdb
        pdb.Pdb().set_trace(frame)

    def start(self,
              frontend_addr=conf.FRONTEND_ADDR,
              backend_addr=conf.BACKEND_ADDR,
              administrative_addr=conf.ADMINISTRATIVE_ADDR):
        """
        Begin listening for connections on the provided connection strings

        Args:
            frontend_addr (str): connection string to listen for requests
            backend_addr (str): connection string to listen for workers
            administrative_addr (str): connection string to listen for emq-cli
                commands on.
        """
        self.status = STATUS.starting

        self.incoming.listen(frontend_addr)
        self.outgoing.listen(backend_addr)
        self.administrative_socket.listen(administrative_addr)

        self.status = STATUS.listening
        logger.info('Listening for requests on {}'.format(frontend_addr))
        logger.info('Listening for workers on {}'.format(backend_addr))
        logger.info('Listening for administrative commands on {}'.format(
            administrative_addr))

        self._start_event_loop()

    def _start_event_loop(self):
        """
        Starts the actual eventloop. Usually called by :meth:`Router.start`
        """
        while True:
            if self.received_disconnect:
                break
            now = monotonic()
            events = self.poller.poll()

            if events.get(self.administrative_socket) == poller.POLLIN:
                msg = self.administrative_socket.recv_multipart()
                if conf.SUPER_DEBUG:
                    logger.debug('ADMIN: {}'.format(msg))
                # ##############
                # Admin Commands
                # ##############
                if len(msg) > 4:
                    if msg[3] == DISCONNECT:
                        logger.info('Received DISCONNECT from administrator')
                        self.send_ack(
                            self.administrative_socket, msg[0], msg[4])
                        self.on_disconnect(msg[4], msg)
                    elif msg[3] == 'STATUS':
                        sendmsg(self.administrative_socket, msg[0], 'REPLY',
                                (self.get_status(),))
                    elif msg[3] == ROUTER_SHOW_WORKERS:
                        sendmsg(self.administrative_socket, msg[0], 'REPLY',
                                (self.get_workers_status(),))
                    elif msg[3] == ROUTER_SHOW_SCHEDULERS:
                        sendmsg(self.administrative_socket, msg[0], 'REPLY',
                                (self.get_schedulers_status(),))

            if events.get(self.incoming) == poller.POLLIN:
                msg = self.incoming.recv_multipart()
                self.handle_wal_log(msg)
                self.process_client_message(msg)

            if events.get(self.outgoing) == poller.POLLIN:
                msg = self.outgoing.recv_multipart()
                self.process_worker_message(msg)

            # TODO: Optimization: the calls to functions could be done in
            #     another thread so they don't block the loop. synchronize
            if not conf.DISABLE_HEARTBEATS:
                # Send a HEARTBEAT if necessary
                if now - self._meta['last_sent_heartbeat'] >= \
                   conf.HEARTBEAT_INTERVAL:
                    self.send_workers_heartbeats()

                if now - self._meta['last_worker_cleanup'] >= 10:
                    # Loop through the next worker queue and clean up any dead
                    # ones so the next one is alive
                    self.clean_up_dead_workers()

                if now - self._meta['last_sent_scheduler_heartbeat'] >= \
                   conf.HEARTBEAT_INTERVAL:
                    self.send_schedulers_heartbeats()

                if now - self._meta['last_scheduler_cleanup'] >= 10:
                    self.clean_up_dead_schedulers()

    def reset_heartbeat_counters(self):
        """
        Reset all the counters for heartbeats back to 0
        """
        super(Router, self).reset_heartbeat_counters()

        # track the last time the router sent a heartbeat to the schedulers
        self._meta['last_sent_scheduler_heartbeat'] = 0

    def send_ack(self, socket, recipient, msgid):
        """
        Sends an ACK response

        Args:
            socket (socket): The socket to use for this ack
            recipient (str): The recipient id for the ack
            msgid: The unique id that we are acknowledging

        Returns:
            msgid: The ID of the ACK message
        """
        logger.info('Sending ACK to %s' % recipient)
        logger.info('Queue information %s' % self.queues)
        logger.info('Worker information %s' % self.workers)
        msgid = sendmsg(socket, recipient, 'ACK', msgid)

        return msgid

    def send_kbye(self, socket, recipient):
        logger.info('Sending {} to {}'.format(KBYE, recipient))
        msg_id = sendmsg(socket, recipient, KBYE)
        return msg_id

    def send_heartbeat(self, socket, recipient):
        """
        Custom send heartbeat method to take into account the recipient that is
        needed when building messages

        Args:
            socket (socket): the socket to send the heartbeat with
            recipient (str): Worker I

        Returns:
            msgid: The ID of the HEARTBEAT message
        """
        msgid = sendmsg(socket, recipient, 'HEARTBEAT', str(timestamp()))

        return msgid

    def send_workers_heartbeats(self):
        """
        Send HEARTBEATs to all registered workers.
        """
        self._meta['last_sent_heartbeat'] = monotonic()

        for worker_id in self.workers:
            self.send_heartbeat(self.outgoing, worker_id)

    def send_schedulers_heartbeats(self):
        """
        Send HEARTBEATs to all registered schedulers
        """
        self._meta['last_sent_scheduler_heartbeat'] = monotonic()

        for scheduler_id in self.schedulers:
            self.send_heartbeat(self.incoming, scheduler_id)

    def on_heartbeat(self, sender, msgid, msg):
        """
        a placeholder for a no-op command. The actual 'logic' for HEARTBEAT is
        in :meth:`self.process_worker_message` because any message from a
        worker counts as a HEARTBEAT
        """

    def on_inform(self, sender, msgid, msg):
        """
        Handles an INFORM message. This happens when new worker coming online
        and announces itself.
        """
        queue_names = msg[0]
        client_type = msg[1]

        if not queue_names:  # Ideally, this matches some workers
            queues = conf.QUEUES
        else:
            try:
                queues = list(map(tuplify, json.loads(queue_names)))
            except ValueError:
                # this was invalid json
                logger.error(
                    'Received invalid queue names in INFORM. names:{} from:{} '
                    'type:{}'.format(
                        queue_names, sender, client_type))
                return

        logger.info('Received INFORM request from {} (type: {})'.format(
            sender, client_type))

        if client_type == CLIENT_TYPE.worker:
            self.add_worker(sender, queues)
            self.send_ack(self.outgoing, sender, msgid)
        elif client_type == CLIENT_TYPE.scheduler:
            self.add_scheduler(sender)
            self.send_ack(self.incoming, sender, msgid)

    def on_reply(self, sender, msgid, msg):
        """
        Handles an REPLY message. Replies are sent by the worker for latanecy
        measurements
        """

        orig_msgid = msg[1]
        if conf.SUPER_DEBUG:
            logger.debug('Received REPLY from {} (msgid: {}, ACK msgid: {})'.
                         format(sender, msgid, orig_msgid))

        if orig_msgid in self.job_latencies:
            elapsed_secs = (monotonic()
                            - self.job_latencies[orig_msgid][0]) * 1000.0
            logger.info("Completed {queue} job with msgid: {msgid} in "
                        "{time:.2f}ms".format(
                            queue=self.job_latencies[orig_msgid][1],
                            msgid=orig_msgid,
                            time=elapsed_secs))
            del self.job_latencies[orig_msgid]

    def on_disconnect(self, msgid, msg):
        """
        Prepare router for disconnecting by removing schedulers, clearing
        worker queue (if needed), and removing workers.
        """

        # Remove schedulers and send them a kbye
        logger.info("Router preparing to disconnect...")
        for scheduler in self.schedulers:
            self.send_kbye(self.incoming, scheduler)

        self.schedulers.clear()
        self.incoming.unbind(conf.FRONTEND_ADDR)

        if len(self.waiting_messages) > 0:
            logger.info("Router processing messages in queue.")
            for queue in self.waiting_messages.keys():
                while not self.waiting_messages[queue].is_empty():
                    msg = self.waiting_messages[queue].popleft()
                    self.process_worker_message(msg)

        for worker in self.workers.keys():
            self.send_kbye(self.outgoing, worker)

        self.workers.clear()
        self.outgoing.unbind(conf.BACKEND_ADDR)

        # Loops event loops should check for this and break out
        self.received_disconnect = True

    def on_ready(self, sender, msgid, msg):
        """
        A worker that we should already know about is ready for another job

        Args:
            sender (str): The id of the sender
            msgid (str): Unique identifier for this message
            msg: The actual message that was sent
        """
        queue_names = self.workers[sender]['queues']

        # if there are waiting messages for the queues this worker is a member
        # of, then reply back with the oldest waiting message, otherwise just
        # add the worker to the list of available workers.
        # Note: This is only taking into account the queue the worker is
        # returning from, and not other queue_names that might have had
        # messages waiting even longer.
        # Assumes the highest priority queue comes first
        for queue in queue_names:
            queue_name = queue[1]
            if queue_name in list(self.waiting_messages.keys()):
                logger.debug('Found waiting message in the %s waiting_messages'
                             ' queue' % queue_name)
                msg = self.waiting_messages[queue_name].peekleft()

                try:
                    fwdmsg(self.outgoing, sender, msg)
                    self.waiting_messages[queue_name].popleft()
                except exceptions.PeerGoneAwayError:
                    # Cleanup a workerg that cannot be contacted, leaving the
                    # message in queue
                    self.workers[sender]['hb'] = 0
                    self.clean_up_dead_workers()

                # It is easier to check if a key exists rather than the len of
                # a key's value if it exists elsewhere, so if that was the last
                # message remove the queue
                if len(self.waiting_messages[queue_name]) == 0:
                    logger.debug('No more messages in waiting_messages queue '
                                 '%s. Removing from list...' % queue_name)
                    del self.waiting_messages[queue_name]

                # the message has been forwarded so short circuit that way the
                # manager isn't reslotted
                return

        self.requeue_worker(sender)

    def on_request(self, sender, msgid, msg, depth=1):
        """
        Process a client REQUEST frame

        Args:
            sender
            msgid
            msgid
            depth (int): The recusion depth in retrying when PeerGoneAwayError
                is raised.
        """
        import psutil

        try:
            queue_name = msg[0]
        except IndexError:
            logger.exception("Queue name undefined. Sender {}; MsgID: {}; "
                             "Msg: {}".format(sender, msgid, msg))
            return

        # If we have no workers for the queue assign it to the default queue
        if queue_name not in self.queues:
            logger.warning("Received REQUEST with a queue I don't recognize: "
                           "%s. Sending to default queue." % (queue_name,))
            queue_name = conf.DEFAULT_QUEUE_NAME

        self.job_latencies[msgid] = (monotonic(), queue_name)

        try:
            worker_addr = self.get_available_worker(queue_name=queue_name)
        except (exceptions.NoAvailableWorkerSlotsError,
                exceptions.UnknownQueueError):
            logger.warning('No available workers for queue "%s". '
                           'Buffering message to send later.' % queue_name)
            if queue_name not in self.waiting_messages:
                # Since the default queue will pick up messages with invalid
                # queues, it will need to be larger than other queues
                if queue_name == conf.DEFAULT_QUEUE_NAME:
                    total_mem = psutil.virtual_memory().total
                    # Set queue limit to be 75% of total memory with ~100 byte
                    # messages
                    limit = int(int(total_mem / 100) * 0.75)
                    self.waiting_messages[queue_name] = EMQdeque(
                        full=limit, on_full=router_on_full)
                else:
                    self.waiting_messages[queue_name] = \
                        EMQdeque(full=conf.HWM,
                                 on_full=router_on_full)

            if self.waiting_messages[queue_name].append(
                    ['', constants.PROTOCOL_VERSION, 'REQUEST',
                     msgid, ] + msg):
                logger.debug('%d waiting messages in queue "%s"' %
                             (len(self.waiting_messages[queue_name]),
                              queue_name))
            else:
                logger.warning('High Watermark {} met for {}, notifying'.
                               format(conf.HWM, queue_name))
            return

        try:
            # Check if msg type is for executing function
            self.job_latencies[msgid] = (monotonic(), queue_name)

            # Rebuild the message to be sent to the worker. fwdmsg will
            # properly address the message.
            if queue_name not in self.processed_message_counts:
                self.processed_message_counts[queue_name] = 1
            else:
                self.processed_message_counts[queue_name] += 1

            if worker_addr not in self.processed_message_counts_by_worker:
                self.processed_message_counts_by_worker[worker_addr] = 1
            else:
                self.processed_message_counts_by_worker[worker_addr] += 1

            fwdmsg(self.outgoing, worker_addr, ['', constants.PROTOCOL_VERSION,
                                                'REQUEST', msgid, ] + msg)

            self.workers[worker_addr]['available_slots'] -= 1
            # Acknowledgment of the request being submitted to the client
            sendmsg(self.incoming, sender, 'REPLY',
                    (msgid,))
        except exceptions.PeerGoneAwayError:
            logger.debug(
                "Worker {} has unexpectedly gone away. Removing this worker "
                "before trying another worker".format(worker_addr))

            # Remove this worker to prevent infinite loop
            self.workers[worker_addr]['hb'] = 0
            self.clean_up_dead_workers()

            # Recursively try again. TODO: are there better options?
            self.process_client_message(
                [sender, '', PROTOCOL_VERSION, 'REQUEST', msgid] + msg,
                depth=depth + 1)

    def clean_up_dead_workers(self):
        """
        Loops through the worker queues and removes any workers who haven't
        responded in HEARTBEAT_TIMEOUT
        """
        now = monotonic()
        self._meta['last_worker_cleanup'] = now

        # Because workers and queues are removed from inside a loop, a copy is
        # needed to prevent the dict we are iterating over from changing.
        workers = copy(self.workers)
        queues = copy(self.queues)

        for worker_id in workers:
            last_hb_seconds = now - self.workers[worker_id]['hb']
            if last_hb_seconds >= conf.HEARTBEAT_TIMEOUT:
                logger.info("No messages from worker {} in {}. Removing from "
                            "the queue. TIMEOUT: {}".format(
                                worker_id, last_hb_seconds,
                                conf.HEARTBEAT_TIMEOUT))

                # Remove the worker from the actual queues
                for queue in self.workers[worker_id]['queues']:
                    try:
                        self.queues[queue[1]].remove((queue[0], worker_id))
                    except KeyError:
                        # This queue disappeared for some reason
                        continue

                del self.workers[worker_id]

        # Remove the empty queue
        for queue_name in queues:
            if len(self.queues[queue_name]) == 0:
                del self.queues[queue_name]

    def add_worker(self, worker_id, queues=None):
        """
        Adds a worker to worker queues

        Args:
            worker_id (str): unique id of the worker to add
            queues: queue or queues this worker should be a member of
        """
        if queues and not isinstance(queues, (list, tuple)):
            raise TypeError('type of `queue` parameter not one of (list, '
                            'tuple). got {}'.format(type(queues)))

        if worker_id in self.workers:
            logger.warning('Worker id already found in `workers`. Overwriting '
                           'data')

        # Add the worker to our worker dict
        self.workers[worker_id] = {}
        self.workers[worker_id]['queues'] = tuple(queues)
        self.workers[worker_id]['hb'] = monotonic()
        self.workers[worker_id]['available_slots'] = 0

        # Define priorities. First element is the highest priority
        for q in queues:
            if q[1] not in self.queues:
                self.queues[q[1]] = list()

            self.queues[q[1]].append((q[0], worker_id))
            self.queues[q[1]] = self.prioritize_queue_list(self.queues[q[1]])

        logger.debug('Added worker {} to the queues {}'.format(
            worker_id, queues))

    def get_available_worker(self, queue_name=conf.DEFAULT_QUEUE_NAME):
        """
        Gets the job manager with the next available worker for the provided
        queue.

        Args:
            queue_name (str): Name of the queue

        Raises:
            NoAvailableWorkerSlotsError: Raised when there are no available
            slots in any the job managers.
            UnknownQueueError: Raised when ``queue_name`` is not found in
                self.queues

        Returns:
            (str): uuid of the job manager with an available worker slot
        """
        if queue_name not in self.queues:
            logger.warning("unknown queue name: {} - Discarding message.".
                           format(queue_name))
            raise exceptions.UnknownQueueError('Unknown queue name {}'.format(
                queue_name
            ))

        popped_workers = []
        worker_addr = None
        while not worker_addr and len(self.queues[queue_name]) > 0:
            try:
                # pop the next job manager id & check if it has a worker slot
                # if it doesn't add it to popped_workers to be added back to
                # self.queues after the loop
                worker = self.queues[queue_name].pop(0)

                # LRU when sorted later by appending
                popped_workers.append(worker)

                if self.workers[worker[1]]['available_slots'] > 0:
                    worker_addr = worker[1]
                    break

            except KeyError:
                # This should only happen if worker[1] is missing 1 from
                # self.workers because:
                #  - available slots initialized to 0 self.add_worker()
                #  - we already checked that self.queues[queue_name] exists
                logger.error("Worker {} not found for queue {}".format(
                    worker, queue_name))
                logger.debug("Tried worker {} in self.workers for queue {} "
                             "but it wasn't found in self.workers".format(
                                 worker, queue_name
                             ))
                continue

            except IndexError:
                # worker[1] should exist if it follows the (priority, id) fmt
                logger.error("Invalid priority/worker format in self.queues "
                             "{}".format(worker))
                continue
        else:
            # No more queues to try
            pass

        if popped_workers:
            self.queues[queue_name].extend(popped_workers)
            self.queues[queue_name] = self.prioritize_queue_list(
                self.queues[queue_name])

        if worker_addr:
            return worker_addr
        else:
            raise exceptions.NoAvailableWorkerSlotsError(
                "There are no availabe workers for queue {}. Try again "
                "later".format(queue_name))

    def clean_up_dead_schedulers(self):
        """
        Loops through the list of schedulers and remove any schedulers who
        the router hasn't received a heartbeat in HEARTBEAT_TIMEOUT
        """
        now = monotonic()
        self._meta['last_scheduler_cleanup'] = now
        schedulers = copy(self.scheduler_queue)

        for scheduler_id in schedulers:
            last_hb_seconds = now - self.schedulers[scheduler_id]['hb']
            if last_hb_seconds >= conf.HEARTBEAT_TIMEOUT:
                logger.critical("No HEARTBEAT from scheduler {} in {} Removing"
                                " from the queue".format(scheduler_id,
                                                         last_hb_seconds))
                del self.schedulers[scheduler_id]
                self.scheduler_queue.remove(scheduler_id)

    def add_scheduler(self, scheduler_id):
        """
        Adds a scheduler to the queue to receive SCHEDULE commands

        Args:
            scheduler_id (str): unique id of the scheduler to add
        """
        self.scheduler_queue.append(scheduler_id)
        self.schedulers[scheduler_id] = {}
        self.schedulers[scheduler_id]['hb'] = monotonic()
        logger.debug('Adding {} to self.schedulers'.format(scheduler_id))

    def requeue_worker(self, worker_id):
        """
        Add a worker back to the pools for which it is a member of.
        """
        self.workers[worker_id]['available_slots'] += 1

    def handle_wal_log(self, original_msg):

        try:
            message = parse_router_message(original_msg)
        except exceptions.InvalidMessageError:
            logger.exception('Invalid message from clients: {}'.format(
                str(original_msg)))
            return

        command = message[1]

        if conf.WAL_ENABLED and \
           command in ("REQUEST", "SCHEDULE", "UNSCHEDULE"):
            wal_logger.info(original_msg)

    def process_client_message(self, original_msg, depth=0):
        """
        Args:
            msg: The untouched message from zmq
            depth: The number of times this method has been recursively
                called. This is used to short circuit message retry attempts.

        Raises:
            InvalidMessageError: Unable to parse the message
        """
        # Limit recusive depth (timeout on PeerGoneAwayError)
        if (depth > 100):
            logger.error('Recursion Error: process_client_message called too '
                         'many times with message: {}'.format(original_msg))
            return

        try:
            message = parse_router_message(original_msg)
        except exceptions.InvalidMessageError:
            logger.exception('Invalid message from clients: {}'.format(
                str(original_msg)))
            return

        sender = message[0]
        command = message[1]
        msgid = message[2]
        msg = message[3]

        # Count this message as a heart beat if it came from a scheduler that
        # the router is aware of.

        if sender in self.schedulers and command == KBYE:
            self._remove_scheduler(sender)
            return

        if sender in self.schedulers and sender in self.scheduler_queue:
            self.schedulers[sender]['hb'] = monotonic()

            # If it is a heartbeat then there is nothing left to do
            if command == "HEARTBEAT":
                return

        # REQUEST is the most common message so it goes at the top
        if command == "REQUEST":
            self.on_request(sender, msgid, msg, depth=depth)

        elif command == "INFORM":
            # This is a scheduler trying join
            self.on_inform(sender, msgid, msg)

        elif command == "SCHEDULE":
            # Forward the schedule message to the schedulers
            try:
                scheduler_addr = self.scheduler_queue.pop()
            except IndexError:
                logger.error("Received a SCHEDULE command with no schedulers. "
                             "Discarding.")
                return
            self.scheduler_queue.append(scheduler_addr)
            self.schedulers[scheduler_addr] = {
                'hb': monotonic(),
            }

            try:
                # Strips off the client id before forwarding because the
                # scheduler isn't expecting it.
                fwdmsg(self.incoming, scheduler_addr, original_msg[1:])

            except exceptions.PeerGoneAwayError:
                logger.debug("Scheduler {} has unexpectedly gone away. Trying "
                             "another scheduler.".format(scheduler_addr))
                self.process_client_message(original_msg[1:], depth + 1)

        elif command == "UNSCHEDULE":
            # Forward the unschedule message to all schedulers
            for scheduler_addr, scheduler in self.schedulers.items():
                self.schedulers[scheduler_addr] = {
                    'hb': monotonic(),
                }

                try:
                    # Strips off the client id before forwarding because the
                    # scheduler isn't expecting it.
                    fwdmsg(self.incoming, scheduler_addr, original_msg[1:])

                except exceptions.PeerGoneAwayError:
                    logger.debug("Scheduler {} has unexpectedly gone away."
                                 " Schedule may still exist.".
                                 format(scheduler_addr))
                    self.process_client_message(original_msg[1:], depth + 1)

        elif command == DISCONNECT:
            self.on_disconnect(msgid, msg)

    def process_worker_message(self, msg):
        """
        This method is called when a message comes in from the worker socket.
        It then calls `on_COMMAND.lower()`. If `on_command` isn't found, then
        a warning is created.

        Args:
            msg: The untouched message from zmq
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

        if sender in self.workers:
            if command.upper() == KBYE:
                self._remove_worker(sender)
            # Treat any other message like a HEARTBEAT.
            else:
                self.workers[sender]['hb'] = monotonic()
        elif command.lower() != 'inform':
            logger.critical('Unknown worker %s attempting to run %s command: '
                            '%s' % (sender, command, str(msg)))
            return

        if hasattr(self, "on_%s" % command.lower()):
            func = getattr(self, "on_%s" % command.lower())
            func(sender, msgid, message)

    def _remove_worker(self, worker_id):
        """
        Remove worker with given id from any queues it belongs to.

        Args:
            worker_id: (str) ID of worker to remove
        """
        worker = self.workers.pop(worker_id)
        for queue in worker['queues']:
            name = queue[1]
            workers = self.queues[name]
            revised_list = [x for x in workers if x[1] != worker_id]
            self.queues[name] = revised_list
            logger.debug('Removed worker - {} from {}'.format(worker_id, name))

    def _remove_scheduler(self, scheduler_id):
        """
        Remove scheduler with given id from registered schedulers.

        Args:
            scheduler_id: (str) ID of scheduler to remove
        """
        self.schedulers.pop(scheduler_id)
        schedulers_to_remove = self.scheduler_queue
        self.scheduler_queue = \
            [x for x in schedulers_to_remove if x != scheduler_id]
        logger.debug('Removed scheduler - {} from known schedulers'.format(
            scheduler_id))

    @classmethod
    def prioritize_queue_list(cls, unprioritized_iterable):
        """
        Prioritize a given iterable in the format: ((PRIORITY, OBJ),..)

        Args:
            unprioritized_iterable (iter): Any list, tuple, etc where the
                0-index key is an integer to use as priority. Largest numbers
                come first.

        Raises:
            IndexError - There was no 0-index element.

        Returns:
            decsending order list. E.g. ((20, 'a'), (14, 'b'), (12, 'c'))
        """
        return sorted(unprioritized_iterable, key=lambda x: x[0], reverse=True)

    def get_status(self):
        """
        Return
           (str) Serialized information about the current state of the router.
        """
        queue_latency_list = {}
        queue_latency_count = {}
        queue_max_latency_list = {}
        queue_waiting_list = {}

        now = monotonic()

        for job in self.job_latencies:
            queue = self.job_latencies[job][1]
            latency = self.job_latencies[job][0]

            if queue not in queue_latency_list:
                queue_latency_list[queue] = latency
                queue_latency_count[queue] = 1
            else:
                queue_latency_list[queue] += latency
                queue_latency_count[queue] += 1

            if queue not in queue_max_latency_list:
                queue_max_latency_list[queue] = latency
            else:
                if queue_max_latency_list[queue] > latency:
                    queue_max_latency_list[queue] = latency

        for queue in queue_latency_list:
            queue_latency_list[queue] = int(
                (now - (queue_latency_list[queue] /
                        max(queue_latency_count[queue], 1))) * 1000)

        for queue in queue_max_latency_list:
            queue_max_latency_list[queue] = int(
                (now - queue_max_latency_list[queue]) * 1000)

        for queue in self.waiting_messages:
            queue_waiting_list[queue] = len(self.waiting_messages[queue])

        return json.dumps({
            'inflight_messages_by_queue': queue_latency_count,
            'latency_messages_by_queue': queue_latency_list,
            'max_latency_messages_by_queue': queue_max_latency_list,
            'waiting_messages_by_queue': queue_waiting_list,
            'processed_messages_by_queue': self.processed_message_counts,
            'processed_messages_by_worker': self.processed_message_counts_by_worker  # noqa
        })

    def get_workers_status(self):
        return json.dumps({
            'connected_workers': self.workers,
            'connected_queues': self.queues
        })

    def get_schedulers_status(self):
        return json.dumps({
            'connected_schedulers': self.schedulers
        })

    def sighup_handler(self, signum, frame):
        """
        Reloads the configuration and rebinds the ports. Exectued when the
        process receives a SIGHUP from the system.
        """
        logger.info('Caught signame %s' % signum)
        self.incoming.unbind(conf.FRONTEND_ADDR)
        self.outgoing.unbind(conf.BACKEND_ADDR)
        import_settings()
        self.start(frontend_addr=conf.FRONTEND_ADDR,
                   backend_addr=conf.BACKEND_ADDR,
                   administrative_addr=conf.ADMINISTRATIVE_ADDR)

    def router_main(self):
        """
        Kick off router with logging and settings import
        """
        import_settings()
        setup_wal_logger('eventmq-wal', conf.WAL)
        self.start(frontend_addr=conf.FRONTEND_ADDR,
                   backend_addr=conf.BACKEND_ADDR,
                   administrative_addr=conf.ADMINISTRATIVE_ADDR)


def router_on_full():
    logger.critical('High watermark hit in router')


# Entry point for pip console scripts
def router_main():
    Router()
