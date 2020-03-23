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
from imp import reload
import json
import unittest

from freezegun import freeze_time
import mock
from six.moves import range
from testfixtures import LogCapture
import zmq

from eventmq import conf, constants, exceptions, receiver, router
from eventmq.utils.classes import EMQdeque
from eventmq.utils.timeutils import monotonic


class TestCase(unittest.TestCase):
    def setUp(self):
        self.router = router.Router(skip_signal=True)
        self.router.zcontext = mock.Mock(spec=zmq.Context)
        self.router.incoming = mock.Mock(spec=receiver.Receiver)
        self.router.outgoing = mock.Mock(spec=receiver.Receiver)

    @mock.patch('eventmq.receiver.zmq.Socket.bind')
    @mock.patch('eventmq.router.Router._start_event_loop')
    def test_start(self, event_loop_mock, zsocket_bind_mock):
        # set the config file back to defaults. prevents tests from failing
        # when there is a config file on the filesystem
        reload(conf)

        # Test default args
        self.router.start()
        self.router.incoming.listen.assert_called_with(conf.FRONTEND_ADDR)
        self.router.outgoing.listen.assert_called_with(conf.BACKEND_ADDR)
        self.assertEqual(self.router.status, constants.STATUS.listening)

        # Test invalid args
        # self.router.start(frontend_addr=23, backend_addr=44)

    @mock.patch('eventmq.router.Router.send_ack')
    @mock.patch('eventmq.router.Router.add_worker')
    def test_on_inform_worker(self, add_worker_mock, send_ack_mock):
        sender_id = 'omgsenderid19'
        queues = '[[32, "top"], [23, "drop"], [12, "shop"]]'
        inform_msgid = 'msg31'

        self.router.on_inform(
            sender_id, inform_msgid, [queues, 'worker'])

        self.router.send_ack.assert_called_with(
            self.router.outgoing, sender_id, inform_msgid)

        self.router.add_worker.assert_called_with(
            sender_id, [(32, 'top'), (23, 'drop'), (12, 'shop')])

    @mock.patch('eventmq.router.Router.send_ack')
    @mock.patch('eventmq.router.Router.add_worker')
    def test_on_inform_worker_default_queue(self, add_worker_mock,
                                            send_ack_mock):
        # Test on_inform when no queue is specified
        sender_id = 'omgsender18'
        queues = ''
        inform_msgid = 'msg29'

        conf.QUEUES = [(10, 'default'), ]

        self.router.on_inform(
            sender_id, inform_msgid, [queues, constants.CLIENT_TYPE.worker])

        self.router.send_ack.assert_called_with(
            self.router.outgoing, sender_id, inform_msgid)
        self.router.add_worker.assert_called_with(
            sender_id, [(10, 'default'), ])

    def test_on_inform_invalid_queues(self):
        # https://github.com/enderlabs/eventmq/issues/33
        # when receiving an invalid queue name for inform, the router shouldn't
        # crash
        sender_id = 'sch01'
        msgid = 'msg01'
        msg = ["invalid queue name", constants.CLIENT_TYPE.worker]

        with LogCapture() as log_checker:
            self.router.on_inform(sender_id, msgid, msg)

            log_checker.check(
                ('eventmq.router',
                 'ERROR',
                 'Received invalid queue names in INFORM. names:{} from:{} '
                 'type:{}'.format(msg[0], sender_id, msg[1]))
            )

    # @mock.patch('eventmq.router.Router.prioritize_queue_list')
    def test_add_worker(self):
        worker1_id = 'w1'
        worker2_id = 'w2'

        queues1 = [(10, 'top'), (9, 'drop'), (8, 'shop')]
        queues2 = [(10, 'default'), (9, 'shop'), (8, 'top')]

        self.router.add_worker(worker1_id, queues=queues1)
        self.router.add_worker(worker2_id, queues=queues2)
        # added to the list of workers
        self.assertIn(worker1_id, self.router.workers)
        self.assertIn(worker2_id, self.router.workers)
        self.assertGreater(self.router.workers[worker1_id]['hb'], 0)
        # no slots yet
        self.assertEqual(self.router.workers[worker1_id]['available_slots'], 0)

        # aware of the queues
        self.assertEqual(3, len(self.router.workers[worker1_id]['queues']))
        self.assertIn((10, 'top'), self.router.workers[worker1_id]['queues'])
        self.assertIn((9, 'drop'), self.router.workers[worker1_id]['queues'])
        self.assertIn((8, 'shop'), self.router.workers[worker1_id]['queues'])

        # Worker2
        self.assertIn((10, 'default'),
                      self.router.workers[worker2_id]['queues'])
        self.assertIn((9, 'shop'), self.router.workers[worker2_id]['queues'])
        self.assertIn((8, 'top'), self.router.workers[worker2_id]['queues'])

        self.assertIn((10, worker1_id), self.router.queues['top'])
        self.assertIn((9, worker1_id), self.router.queues['drop'])
        self.assertIn((8, worker1_id), self.router.queues['shop'])

        self.assertIn((10, worker2_id), self.router.queues['default'])
        self.assertIn((9, worker2_id), self.router.queues['shop'])
        self.assertIn((8, worker2_id), self.router.queues['top'])

    def test_add_worker_invalid_queues(self):
        with self.assertRaises(TypeError):
            self.router.add_worker('83902', 8902)

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
                'queues': [(10, 'default'), ],
                'hb': 123.2,
                'available_slots': 3,
            },
            'w2': {
                'queues': [(10, 'not-default'), ],
                'hb': 123.2,
                'available_slots': 2,
            }
        }
        self.router.send_workers_heartbeats()

        # After sending the heartbeat, it should be greater than 0 (note: this
        # is very hard to mock)
        self.assertGreater(self.router._meta['last_sent_heartbeat'], 0)

        send_heartbeat_mock.assert_has_calls(
            [mock.call(self.router.outgoing, 'w1'),
             mock.call(self.router.outgoing, 'w2')], any_order=True)

    @mock.patch('eventmq.router.Router.send_heartbeat')
    def test_send_schedulers_heartbeats(self, send_hb_mock):
        scheduler_id = 's39'
        self.assertEqual(self.router._meta['last_sent_scheduler_heartbeat'], 0)

        self.router.schedulers = {
            scheduler_id: {
                'hb': 0,
            }
        }

        self.router.send_schedulers_heartbeats()

        self.assertGreater(
            self.router._meta['last_sent_scheduler_heartbeat'], 0)
        send_hb_mock.assert_called_with(self.router.incoming, scheduler_id)

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
                'queues': [(10, 'default'), ],
                'hb': 123.2,
                'available_slots': 3,
            },
        }

        self.router.waiting_messages['default'] = EMQdeque(
            initial=[waiting_msg, ])

        self.router.on_ready(worker_id, msgid, msg)

        fwdmsg_mock.assert_called_with(self.router.outgoing, worker_id,
                                       waiting_msg)

        self.router.on_ready(worker_id, msgid + 'a', msg)
        self.router.requeue_worker.assert_called_with(worker_id)

    @mock.patch('eventmq.router.fwdmsg')
    @mock.patch('eventmq.router.Router.requeue_worker')
    def test_on_ready_multpile_queues(self, requeue_worker_mock,
                                      fwdmsg_mock):
        # Test that if messages are waiting on multiple queues, they are
        # dispatched immediatly after a READY message.
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
                'queues': [(10, 'kun'), (0, 'blu')],
                'hb': 123.2,
                'available_slots': 0,
            },
            worker2_id: {
                'queues': [(10, 'blu'), (0, 'kun')],
                'hb': 123.2,
                'available_slots': 0
            }
        }

        self.router.queues = {
            'kun': [(10, worker1_id), (0, worker2_id)],
            'blu': [(10, worker2_id), (0, worker1_id)],
        }

        self.router.waiting_messages = {
            'kun': EMQdeque(initial=[waiting_msg1, waiting_msg2]),
            'blu': EMQdeque(initial=[waiting_msg3, ]),
        }

        # Forward waiting_msg1
        ready_msgid1 = 'ready23'
        self.router.on_ready(worker1_id, ready_msgid1, ['READY', ready_msgid1])
        fwdmsg_mock.assert_called_with(self.router.outgoing, worker1_id,
                                       waiting_msg1)

        # Forward waiting_msg3 -- blu is a higher priority for worker2
        ready_msgid3 = 'ready19'
        self.router.on_ready(worker2_id, ready_msgid3, ['READY', ready_msgid3])
        fwdmsg_mock.assert_called_with(self.router.outgoing, worker2_id,
                                       waiting_msg3)

        # Forward waiting_msg2
        ready_msgid2 = 'ready5'
        self.router.on_ready(worker2_id, ready_msgid2, ['READY', ready_msgid2])
        fwdmsg_mock.assert_called_with(self.router.outgoing, worker2_id,
                                       waiting_msg2)

        # There should be no keys because the code checks for their existence
        # to know if there is a waiting message
        self.assertEqual(0, len(list(self.router.waiting_messages.keys())))

        # No waiting messages
        self.router.on_ready(worker1_id, ready_msgid1, ['READY', ready_msgid1])
        requeue_worker_mock.assert_called_with(worker1_id)
        self.router.on_ready(worker2_id, ready_msgid2, ['READY', ready_msgid2])
        requeue_worker_mock.assert_called_with(worker2_id)

    @mock.patch('eventmq.router.Router.clean_up_dead_workers')
    @mock.patch('eventmq.router.Router.process_client_message')
    @mock.patch('eventmq.router.Router.get_available_worker')
    @mock.patch('eventmq.router.fwdmsg')
    def test_on_request(self, fwdmsg_mock, get_worker_mock,
                        process_client_msg_mock, cleanupworkers_mock):
        client_id = 'c1'
        msgid = 'msg18'
        queue = 'default'
        msg = ['red socks', 'hello world']
        worker_id = 'w1'

        get_worker_mock.return_value = worker_id

        self.router.workers = {
            worker_id: {
                'queues': [(10, queue)],
                'hb': 2903.34,
                'available_slots': 1,
            }
        }
        self.router.queues = {
            queue: [(10, worker_id), ]
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

        self.assertIn(queue, self.router.waiting_messages)
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

    def test_get_available_worker(self):
        worker2_id = 'w2'
        worker3_id = 'w3'

        queue1_id = 'default'
        queue2_id = 'jimjam'

        self.router.queues = {
            queue1_id: [(10, worker3_id), (0, worker2_id)],
            queue2_id: [(10, worker2_id)],
        }

        self.router.workers = {
            worker2_id: {
                'queues': [(10, queue2_id), (0, queue1_id)],
                'available_slots': 1,
            },
            worker3_id: {
                'queues': [(10, queue1_id), ],
                'available_slots': 1,
            },
        }

        # Get the next available worker for queue2
        check1 = self.router.get_available_worker(queue_name=queue2_id)
        self.assertEqual(worker2_id, check1)

        # Get the next available worker for queue1
        check2 = self.router.get_available_worker(queue_name=queue1_id)
        self.assertEqual(worker3_id, check2)

        # Pretend worker 3 is doing something
        self.router.workers[worker3_id]['available_slots'] = 0

        # Get the next available worker for queue1
        check3 = self.router.get_available_worker(queue_name=queue1_id)
        self.assertEqual(worker2_id, check3)

    def test_get_available_worker_dont_decrement_slots(self):
        # Once upon a time get_available_worker() decremented the available
        # slots counter and the townsfolk greived
        queue1_id = 'q1'
        worker1_id = 'w1'

        self.router.queues = {
            queue1_id: [(10, worker1_id, ), ]
        }

        self.router.workers = {
            worker1_id: {
                'queues': [(10, queue1_id), ],
                'available_slots': 1,
            }
        }

        self.router.get_available_worker(queue_name=queue1_id)

        self.assertEqual(self.router.workers[worker1_id]['available_slots'], 1)

    def test_requeue_worker(self):
        worker_id = 'w1'

        self.router.workers = {
            worker_id: {
                'available_slots': 1
            }
        }

        self.router.requeue_worker(worker_id)
        self.assertEqual(self.router.workers[worker_id]['available_slots'], 2)

    def test_clean_up_dead_workers(self):
        worker1_id = 'w1'
        worker2_id = 'w2'
        worker3_id = 'w3'

        queue1_id = 'default'
        queue2_id = 'jimjam'
        nonexistent_queue1 = 'nonexistent'

        t = monotonic()

        # To ensure the value was changed later because monotonic() is hard to
        # mock
        self.assertEqual(self.router._meta['last_worker_cleanup'], 0)

        conf.HEARTBEAT_TIMEOUT = 1

        self.router.queues = {
            queue1_id: [(10, worker1_id), (0, worker2_id)],
            queue2_id: [(10, worker3_id), (10, worker2_id)],
        }

        self.router.workers = {
            # 3 in the future
            worker1_id: {
                'queues': [(10, queue1_id), ],
                'hb': t + 3,
                'available_slots': 0,
            },
            # below the timeout

            worker2_id: {
                'queues': [(10, queue2_id), (0, queue1_id)],
                'hb': t - 2,
                'available_slots': 2,
            },
            # below the timeout and a queue missing from self.router.queues
            worker3_id: {
                'queues': [(10, queue2_id), (3, nonexistent_queue1)],
                'hb': t - 2,
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
        queue = [(0, 'd'), (10, 'b'), (0, 'e'), (10, 'a'), (0, 'c')]

        sorted1 = self.router.prioritize_queue_list(queue)
        self.assertEqual([(10, 'b'), (10, 'a'), (0, 'd'), (0, 'e'),
                          (0, 'c')], sorted1)

        pop1 = sorted1.pop(0)
        self.assertEqual(pop1, (10, 'b'))
        sorted1.append(pop1)
        # a, b, d, e, c
        sorted2 = self.router.prioritize_queue_list(sorted1)
        self.assertEqual([(10, 'a'), (10, 'b'), (0, 'd'), (0, 'e'), (0, 'c')],
                         sorted2)
        pop2 = sorted2.pop(0)  # a
        pop3 = sorted2.pop(0)  # b
        pop4 = sorted2.pop(0)  # d
        self.assertEqual(pop4, (0, 'd'))
        self.assertEqual(pop2, (10, 'a'))
        self.assertEqual(pop3, (10, 'b'))
        self.assertEqual([(0, 'e'), (0, 'c')], list(sorted2))

        sorted2.append(pop2)
        sorted2.append(pop4)
        sorted2.append(pop3)

        sorted3 = self.router.prioritize_queue_list(sorted2)

        self.assertEqual(sorted3.pop(0), (10, 'a'))
        self.assertEqual(sorted3.pop(0), (10, 'b'))
        self.assertEqual(sorted3.pop(0), (0, 'e'))
        self.assertEqual(sorted3.pop(0), (0, 'c'))
        self.assertEqual(sorted3.pop(0), (0, 'd'))

    def test_disconnect(self):
        msgid = 'msg53'
        msg = ('goodbye', 'world')
        command = constants.DISCONNECT

        queue1_id = 'default'
        queue2_id = 'scoundrel'
        workers = ('w1', 'w2', 'w3')
        schedulers = ('s1', 's2', 's3')

        self.router.queues = {
            queue1_id: [(10, workers[0]), (0, workers[1])],
            queue2_id: [(10, workers[2]), (0, workers[1])]
        }

        self.router.workers = {
            workers[0]: {
                'queues': [(10, queue1_id), ],
                'hb': monotonic() + 3,
                'available_slots': 0,
            },
            workers[1]: {
                'queues': [(0, queue1_id), (10, queue2_id)],
                'hb': monotonic() + 3,
                'available_slots': 2,
            },
            workers[2]: {
                'queues': [(10, queue2_id), ],
                'hb': monotonic() + 3,
                'available_slots': 0,
            }
        }

        self.router.schedulers = {
            schedulers[0]: {
                'hb': 2903.34,
            },
            schedulers[1]: {
                'hb': 2902.99,
            },
            schedulers[2]: {
                'hb': 2904.00,
            }
        }

        self.router.process_client_message((schedulers[2], '',
                                            constants.PROTOCOL_VERSION,
                                            command, msgid) + msg)
        for i in range(0, 3):
            self.assertNotIn(schedulers[i], self.router.schedulers,
                             "Disconnect failed to remove scheduler {} from "
                             "schedulers.".format(schedulers[i]))
            self.assertNotIn(workers[i], self.router.workers,
                             "Disconnect failed to remove worker {} from "
                             "workers.".format(workers[i]))

        self.assertTrue(self.router.received_disconnect, "Router did not "
                                                         "receive disconnect.")

    def test_handle_kbye_from_scheduler(self):
        msgid = 'msg18'
        msg = ('hello', 'world')
        command = constants.KBYE

        # Scheduler ids
        s1 = 's1'
        s2 = 's2'
        s3 = 's3'

        self.router.schedulers = {
            s1: {
                'hb': 2903.34,
            },
            s2: {
                'hb': 2902.99,
            },
            s3: {
                'hb': 2904.00,
            }
        }

        self.router.scheduler_queue = [s1, s2, s3, s1, s2, s3]
        self.router.process_client_message(
            (s1, '', constants.PROTOCOL_VERSION, command, msgid) +
            msg)
        self.assertNotIn(
            s1, self.router.scheduler_queue,
            'Scheduler not removed. {}'.format(self.router.scheduler_queue))

    def test_handle_kbye_from_worker(self):
        msgid = 'msg10'
        msg = ('hello', 'world')
        command = constants.KBYE

        queue1_id = 'default'
        queue2_id = 'scallywag'

        # Worker ids
        w1 = 'w1'
        w2 = 'w2'
        w3 = 'w3'

        self.router.queues = {
            queue1_id: [(10, w1), (0, w2)],
            queue2_id: [(10, w3), (10, w2)],
        }

        self.router.workers = {
            w1: {
                'queues': [(10, queue1_id), ],
                'hb': monotonic() + 3,
                'available_slots': 0,
            },
            w2: {
                'queues': [(0, queue1_id), (10, queue2_id)],
                'hb': monotonic() + 3,
                'available_slots': 2,
            },
            w3: {
                'queues': [(10, queue2_id), ],
                'hb': monotonic() + 3,
                'available_slots': 0,
            }
        }

        self.router.process_worker_message((w2, '', constants.PROTOCOL_VERSION,
                                            command, msgid) + msg)
        self.assertNotIn(
            w2, self.router.queues[queue1_id],
            "Worker not removed from {}".format(queue1_id))
        self.assertNotIn(
            w2, self.router.queues[queue2_id],
            "Worker not removed from {}".format(queue2_id))

    def test_router_status(self):
        msgid1 = 'msg21'
        msgid2 = 'msg19'
        msgid3 = 'msg6'

        queue1_id = 'default'
        queue2_id = 'scallywag'

        # Worker ids
        w1 = 'w1'
        w2 = 'w2'
        w3 = 'w3'

        waiting_msg1 = ['', constants.PROTOCOL_VERSION, 'REQUEST', msgid1,
                        'kun', '', 'hello world']
        waiting_msg2 = ['', constants.PROTOCOL_VERSION, 'REQUEST', msgid2,
                        'kun', '', 'world hello']
        waiting_msg3 = ['', constants.PROTOCOL_VERSION, 'REQUEST', msgid3,
                        'blu', '', 'goodbye']

        self.router.queues = {
            queue1_id: [(10, w1), (0, w2)],
            queue2_id: [(10, w3), (10, w2)],
        }

        t = monotonic()

        self.router.workers = {
            w1: {
                'queues': [(10, queue1_id), ],
                'hb': t,
                'available_slots': 0,
            },
            w2: {
                'queues': [(0, queue1_id), (10, queue2_id)],
                'hb': t,
                'available_slots': 2,
            },
            w3: {
                'queues': [(10, queue2_id), ],
                'hb': t,
                'available_slots': 0,
            }
        }

        self.router.waiting_messages = {
            'kun': EMQdeque(initial=[waiting_msg1, waiting_msg2]),
            'blu': EMQdeque(initial=[waiting_msg3, ]),
        }

        # hacky, but the serialize/deserialize converts the keys to unicode
        # correctly and what not.
        expected_object = {
            'inflight_messages_by_queue': {},
            'latency_messages_by_queue': {},
            'max_latency_messages_by_queue': {},
            'processed_messages_by_queue': {},
            'processed_messages_by_worker': {},
            'waiting_messages_by_queue': {
                q: len(self.router.waiting_messages[q])
                for q in self.router.waiting_messages
            }
        }
        self.assertEqual(
            json.loads(json.dumps(expected_object)),
            json.loads(self.router.get_status()))

        self.assertEqual(
            json.loads(json.dumps({
                'connected_workers': self.router.workers,
                'connected_queues': self.router.queues
            })),
            json.loads(self.router.get_workers_status()))

        self.assertEqual(
            json.loads(json.dumps({
                'connected_schedulers': self.router.schedulers
            })),
            json.loads(self.router.get_schedulers_status()))
