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
import unittest

from freezegun import freeze_time
import mock
import zmq

from .. import conf, constants, exceptions, receiver, router
from ..utils.classes import EMQdeque
from ..utils.timeutils import monotonic


class TestCase(unittest.TestCase):

    def setUp(self):
        self.router = router.Router(skip_signal=True)
        self.router.zcontext = mock.Mock(spec=zmq.Context)
        self.router.incoming = mock.Mock(spec=receiver.Receiver)
        self.router.outgoing = mock.Mock(spec=receiver.Receiver)

    @mock.patch('eventmq.router.Router._start_event_loop')
    def test_start(self, event_loop_mock):
        # Test default args
        self.router.start()
        self.router.incoming.listen.assert_called_with(conf.FRONTEND_ADDR)
        self.router.outgoing.listen.assert_called_with(conf.BACKEND_ADDR)
        self.assertEqual(self.router.status, constants.STATUS.listening)

        # Test invalid args
        # self.router.start(frontend_addr=23, backend_addr=44)

    @mock.patch('eventmq.router.Router.send_ack')
    @mock.patch('eventmq.router.Router.add_worker')
    def test_on_inform_worker_defaut_queue(self, add_worker_mock,
                                           send_ack_mock):
        sender_id = 'omgsender18'
        queues = ''
        inform_msgid = 'msg29'

        self.router.on_inform(sender_id, inform_msgid,
                              [queues, 'worker'])

        self.router.send_ack.assert_called_with(self.router.outgoing,
                                                sender_id, inform_msgid)
        self.router.add_worker.assert_called_with(sender_id, ('default',))

    @mock.patch('eventmq.router.Router.send_ack')
    @mock.patch('eventmq.router.Router.add_worker')
    def test_on_inform_worker(self, add_worker_mock, send_ack_mock):
        sender_id = 'omgsenderid19'
        queues = 'top,drop,shop'
        inform_msgid = 'msg31'

        self.router.on_inform(sender_id, inform_msgid,
                              [queues, 'worker'])

        self.router.send_ack.assert_called_with(self.router.outgoing,
                                                sender_id, inform_msgid)
        self.router.add_worker.assert_called_with(sender_id,
                                                  queues.split(','))

    @mock.patch('eventmq.utils.messages.generate_msgid')
    def test_send_ack(self, generate_msgid_mock):
        ack_msgid = 'msg12'
        orig_msgid = 'msg3'
        sender_id = 'sender93'

        generate_msgid_mock.return_value = ack_msgid

        self.router.send_ack(self.router.outgoing, sender_id, orig_msgid)

        # Verify that an ACK was sent for the INFORM
        self.router.outgoing.send_multipart.assert_called_with(
            ('ACK', ack_msgid, orig_msgid),
            constants.PROTOCOL_VERSION, _recipient_id=sender_id)

    def test_reset_heartbeat_counters(self):
        self.router._meta['last_sent_scheduler_heartbeat'] = 392

        self.router.reset_heartbeat_counters()
        self.assertEqual(self.router._meta['last_sent_scheduler_heartbeat'], 0)

    @mock.patch('eventmq.utils.messages.generate_msgid')
    @freeze_time("2000-01-01")
    def test_send_heartbeat(self, generate_msgid_mock):
        recipient_id = 'n2'
        msgid = 'msg9'
        ts = 946684800.0  # 2000-01-01

        generate_msgid_mock.return_value = msgid

        self.router.send_heartbeat(self.router.incoming, recipient_id)

        self.router.incoming.send_multipart.assert_called_with(
            ('HEARTBEAT', msgid, str(ts)), constants.PROTOCOL_VERSION,
            _recipient_id=recipient_id)

    @mock.patch('eventmq.router.Router.send_heartbeat')
    def test_send_worker_heartbeats(self, send_heartbeat_mock):

        # last heartbeat should start at 0
        self.assertEqual(self.router._meta['last_sent_heartbeat'], 0)
        self.router.workers = {
            'w1': {
                'queues': ['default', ],
                'hb': 123.2,
                'available_slots': 3,
            },
            'w2': {
                'queues': ['default', ],
                'hb': 123.2,
                'available_slots': 2,
            }
        }
        self.router.send_workers_heartbeats()

        # After sending the heartbeat, it should be greater than 0 (note: this
        # is very hard to mock)
        self.assertGreater(self.router._meta['last_sent_heartbeat'], 0)

        self.router.send_heartbeat.assert_has_calls(
            [mock.call(self.router.outgoing, 'w1'),
             mock.call(self.router.outgoing, 'w2')], any_order=True)

    def test_on_disconnect(self):
        self.assertFalse(self.router.received_disconnect)
        self.router.on_disconnect('msg1', 'derp')
        self.assertTrue(self.router.received_disconnect)

    @mock.patch('eventmq.router.fwdmsg')
    @mock.patch('eventmq.router.Router.requeue_worker')
    def test_on_ready(self, requeue_worker_mock, fwdmsg_mock):
        worker_id = '9f9z'
        msgid = 'msg18'
        msg = []

        waiting_msg = ['', constants.PROTOCOL_VERSION, 'REQUEST', msgid,
                       'default', '', 'hello world']

        self.router.workers = {
            worker_id: {
                'queues': ['default', ],
                'hb': 123.2,
                'available_slots': 3,
            },
        }

        self.router.waiting_messages['default'] = EMQdeque(
            initial=[waiting_msg])

        self.router.on_ready(worker_id, msgid, msg)

        fwdmsg_mock.assert_called_with(self.router.outgoing, worker_id,
                                       waiting_msg)

        self.router.on_ready(worker_id, msgid + 'a', msg)
        self.router.requeue_worker.assert_called_with(worker_id)

    @mock.patch('eventmq.router.fwdmsg')
    @mock.patch('eventmq.router.Router.requeue_worker')
    def test_on_ready_prioritized_queue(self, requeue_worker_mock,
                                        fwdmsg_mock):
        worker1_id = 'w1'
        worker2_id = 'w2'

        msgid1 = 'msg21'
        msgid2 = 'msg19'
        msgid3 = 'msg6'
        waiting_msg1 = ['', constants.PROTOCOL_VERSION, 'REQUEST', msgid1,
                        'kun', '', 'hello world']
        waiting_msg2 = ['', constants.PROTOCOL_VERSION, 'REQUEST', msgid2,
                        'kun', '', 'world hello']
        waiting_msg3 = ['', constants.PROTOCOL_VERSION, 'REQUEST', msgid3,
                        'blu', '', 'goodbye']

        self.router.workers = {
            worker1_id: {
                'queues': ['kun', 'blu'],
                'hb': 123.2,
                'available_slots': 0,
            },
            worker2_id: {
                'queues': ['blu', 'kun'],
                'hb': 123.2,
                'available_slots': 0
            }
        }

        self.router.queues = {
            'kun': EMQdeque(initial=[(10, worker1_id), (0, worker2_id)]),
            'blu': EMQdeque(initial=[(10, worker2_id), (0, worker1_id)])

        }

        self.router.waiting_messages = {
            'kun': EMQdeque(initial=[waiting_msg1, waiting_msg2]),
            'blu': EMQdeque(initial=[waiting_msg3])
        }

        ready_msgid1 = 'ready23'
        self.router.on_ready(worker1_id, ready_msgid1, ['READY', ready_msgid1])
        fwdmsg_mock.assert_called_with(self.router.outgoing, worker1_id,
                                       waiting_msg1)

        ready_msgid2 = 'ready19'
        self.router.on_ready(worker2_id, ready_msgid2, ['READY', ready_msgid2])
        fwdmsg_mock.assert_called_with(self.router.outgoing, worker2_id,
                                       waiting_msg3)

        ready_msgid3 = 'ready5'
        self.router.on_ready(worker2_id, ready_msgid3, ['READY', ready_msgid3])
        fwdmsg_mock.assert_called_with(self.router.outgoing, worker2_id,
                                       waiting_msg2)

        self.router.on_ready(worker1_id, ready_msgid1, ['READY', ready_msgid1])
        requeue_worker_mock.assert_called_with(worker1_id)
        self.router.on_ready(worker2_id, ready_msgid2, ['READY', ready_msgid2])
        requeue_worker_mock.assert_called_with(worker2_id)

    @mock.patch('eventmq.router.Router.process_client_message')
    @mock.patch('eventmq.router.Router.get_available_worker')
    @mock.patch('eventmq.router.fwdmsg')
    def test_on_request(self, fwdmsg_mock, get_worker_mock,
                        process_client_msg_mock):
        client_id = 'c1'
        msgid = 'msg18'
        queue = 'default'
        msg = [queue, 'hello world']
        worker_id = 'w1'

        get_worker_mock.return_value = worker_id

        self.router.workers = {
            worker_id: {
                'queues': EMQdeque(initial=(queue,)),
                'hb': 2903.34,
                'available_slots': 1,
            }
        }
        self.router.queues = {
            'default': EMQdeque(initial=((10, worker_id)))
        }

        # Router accepts job for 1 available slot
        self.router.on_request(client_id, msgid, msg)
        fwdmsg_mock.assert_called_with(self.router.outgoing, worker_id,
                                       ['', constants.PROTOCOL_VERSION,
                                        'REQUEST', msgid, ] + msg)
        self.assertEqual(self.router.workers[worker_id]['available_slots'], 0)

        # Router queues message when there are no workers
        def raise_no_workers(*args, **kwargs):
            raise exceptions.NoAvailableWorkerSlotsError()
        msgid += 'a'
        get_worker_mock.side_effect = raise_no_workers
        self.router.on_request(client_id, msgid, msg)

        self.assertIn(msg[0], self.router.waiting_messages)
        self.assertEqual(list(self.router.waiting_messages[queue])[0],
                         ['', constants.PROTOCOL_VERSION, 'REQUEST',
                          msgid] + msg)

        # Retry when the worker peer has gone away
        def raise_peer_gone_away(*args, **kwargs):
            raise exceptions.PeerGoneAwayError()
        get_worker_mock.side_effect = None
        fwdmsg_mock.side_effect = raise_peer_gone_away
        self.router.workers[worker_id]['availabe_slots'] = 1
        msgid = msgid[:-1] + 'b'

        self.router.on_request(client_id, msgid, msg)
        process_client_msg_mock.assert_called_with(
            [client_id, '', constants.PROTOCOL_VERSION, 'REQUEST', msgid]+msg,
            depth=2)

    def test_cleanup_dead_workers(self):
        worker1_id = 'w1'
        worker2_id = 'w2'
        worker3_id = 'w3'

        queue1_id = 'default'
        queue2_id = 'jimjam'
        nonexistent_queue1 = 'pig'

        # To ensure the value was changed later because monotonic() is hard to
        # mock
        self.assertEqual(self.router._meta['last_worker_cleanup'], 0)

        conf.HEARTBEAT_TIMEOUT = 1

        self.router.queues = {
            queue1_id: [(10, worker1_id), (0, worker2_id)],
            queue2_id: [(10, worker3_id), (10, worker2_id)],
        }

        self.router.workers = {
            # 1 second away from timeout
            worker1_id: {
                'queues': (queue1_id,),
                'hb': monotonic() - conf.HEARTBEAT_TIMEOUT + 1,
                'available_slots': 0,
            },
            # below the timeout
            worker2_id: {
                'queues': (queue2_id, queue1_id),
                'hb': 0,
                'available_slots': 2,
            },
            # below the timeout and a queue missing from self.router.queues
            worker3_id: {
                'queues': (queue2_id, nonexistent_queue1),
                'hb': 0,
                'available_slots': 0,
            },
        }

        self.router.clean_up_dead_workers()

        self.assertIn(worker1_id, self.router.workers)
        self.assertNotIn(worker2_id, self.router.workers)
        self.assertNotIn(worker3_id, self.router.workers)

        self.assertIn(queue1_id, self.router.queues)
        self.assertNotIn(queue2_id, self.router.queues)
        self.assertNotIn(nonexistent_queue1, self.router.queues)

    # @mock.patch('eventmq.router.Router.prioritize_queue_list')
    def test_add_worker(self):
        worker1_id = 'w1'
        queues = ('top', 'drop', 'shop')

        self.router.add_worker(worker1_id, queues=queues)
        # added to the list of workers
        self.assertIn(worker1_id, self.router.workers)
        # got an inital heartbeat
        self.assertGreater(self.router.workers[worker1_id]['hb'], 0)
        # no slots yet
        self.assertEqual(self.router.workers[worker1_id]['available_slots'], 0)
        # aware of the queues
        self.assertEqual(3, len(self.router.workers[worker1_id]['queues']))
        self.assertIn((10, worker1_id), list(self.router.queues['top']))
        self.assertIn((0, worker1_id), list(self.router.queues['drop']))
        self.assertIn((0, worker1_id), list(self.router.queues['shop']))

    def test_get_available_worker(self):
        worker2_id = 'w2'
        worker3_id = 'w3'

        queue1_id = 'default'
        queue2_id = 'jimjam'

        self.router.queues = {
            queue1_id: EMQdeque(initial=[(10, worker3_id), (0, worker2_id)]),
            queue2_id: EMQdeque(initial=[(10, worker2_id)]),
        }

        self.router.workers = {
            worker2_id: {
                'queues': (queue2_id, queue1_id),
                'available_slots': 1,
            },
            worker3_id: {
                'queues': (queue1_id,),
                'available_slots': 1,
            },
        }

        # worker1 has no available slots.
        check1 = self.router.get_available_worker(queue_name=queue2_id)
        self.assertEqual(worker2_id, check1)
        self.assertEqual(self.router.workers[worker2_id]['available_slots'], 1)

        check2 = self.router.get_available_worker(queue_name=queue1_id)
        self.assertEqual(worker3_id, check2)
        self.assertEqual(self.router.workers[worker3_id]['available_slots'], 1)

        self.router.workers[worker3_id]['available_slots'] = 0

        check3 = self.router.get_available_worker(queue_name=queue1_id)
        self.assertEqual(worker2_id, check3)
        self.assertEqual(self.router.workers[worker2_id]['available_slots'], 1)

    def test_requeue_worker(self):
        worker_id = 'w1'

        self.router.workers = {
            worker_id: {
                'available_slots': 1
            }
        }

        self.router.requeue_worker(worker_id)
        self.assertEqual(self.router.workers[worker_id]['available_slots'], 2)

    @mock.patch('eventmq.router.Router.on_inform')
    @mock.patch('eventmq.router.Router.on_request')
    @mock.patch('eventmq.router.parse_router_message')
    def test_process_client_message(self, parse_msg_mock, on_request_mock,
                                    on_inform_mock):
        sender_id = 'c4'
        msgid = 'msg11'
        msg = ('hello', 'world')

        command = 'REQUEST'
        parse_msg_mock.return_value = (sender_id, command, msgid, msg)
        self.router.process_client_message(
            (sender_id, '', constants.PROTOCOL_VERSION, command, msgid) +
            msg)
        on_request_mock.assert_called_with(sender_id, msgid, msg, depth=0)

        command = 'INFORM'
        parse_msg_mock.return_value = (sender_id, command, msgid, msg)
        self.router.process_client_message(
            (sender_id, '', constants.PROTOCOL_VERSION, command, msgid) +
            msg)
        on_inform_mock.assert_called_with(sender_id, msgid, msg)

        # command = 'SCHEDULE'
        # parse_msg_mock.return_value = (sender_id, command, msgid, msg)
        # self.router.process_client_message(
        #     (sender_id, '', constants.PROTOCOL_VERSION, command, msgid) +
        #     msg)
        # on_request_mock.assert_called_with(sender_id, msgid, msg)

        # command = 'UNSCHEDULE'
        # parse_msg_mock.return_value = (sender_id, command, msgid, msg)
        # self.router.process_client_message(
        #     (sender_id, '', constants.PROTOCOL_VERSION, command, msgid) +
        #     msg)
        # on_request_mock.assert_called_with(sender_id, msgid, msg)

    @mock.patch('eventmq.router.Router.on_inform')
    @mock.patch('eventmq.router.parse_router_message')
    def test_process_worker_message(self, parse_msg_mock, on_inform_mock):
        sender_id = 'c4'
        msgid = 'msg11'
        msg = ('hello', 'world')

        command = 'INFORM'
        parse_msg_mock.return_value = (sender_id, command, msgid, msg)
        self.router.process_worker_message(
            (sender_id, '', constants.PROTOCOL_VERSION, command, msgid) +
            msg)
        on_inform_mock.assert_called_with(sender_id, msgid, msg)

    def test_prioritize_queue_list(self):
        queue = EMQdeque(initial=[(0, 'd'), (10, 'b'), (0, 'e'), (10, 'a'),
                                  (0, 'c')])

        sorted1 = self.router.prioritize_queue_list(queue)
        self.assertEqual([(10, 'b'), (10, 'a'), (0, 'd'), (0, 'e'),
                          (0, 'c')], list(sorted1))

        pop1 = sorted1.popleft()
        self.assertEqual(pop1, (10, 'b'))
        sorted1.append(pop1)
        # a, b, d, e, c
        sorted2 = self.router.prioritize_queue_list(sorted1)
        self.assertEqual([(10, 'a'), (10, 'b'), (0, 'd'), (0, 'e'), (0, 'c')],
                         list(sorted2))
        pop2 = sorted2.popleft()  # a
        pop3 = sorted2.popleft()  # b
        pop4 = sorted2.popleft()  # d
        self.assertEqual(pop4, (0, 'd'))
        self.assertEqual(pop2, (10, 'a'))
        self.assertEqual(pop3, (10, 'b'))
        self.assertEqual([(0, 'e'), (0, 'c')], list(sorted2))

        sorted2.appendleft(pop2)
        sorted2.appendleft(pop4)
        sorted2.appendleft(pop3)

        # a, b, d, e, c
        sorted3 = self.router.prioritize_queue_list(sorted2)

        self.assertEqual(sorted3.popleft(), (10, 'b'))
        self.assertEqual(sorted3.popleft(), (10, 'a'))
        self.assertEqual(sorted3.popleft(), (0, 'd'))
        self.assertEqual(sorted3.popleft(), (0, 'e'))
        self.assertEqual(sorted3.popleft(), (0, 'c'))
