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
import json
import threading
import time
import unittest

from .utils import FakeDevice
from .. import conf, constants, jobmanager
from ..utils.messages import send_emqp_router_message

ADDR = 'inproc://pour_the_rice_in_the_thing'


class TestCase(unittest.TestCase):
    jm = None

    def setUp(self):
        if self.jm:
            self.jm.on_disconnect(None, None)

        self.jm = jobmanager.JobManager(skip_signal=True)

        # Since JobManager runs as a process a thread is used to allow the loop
        # to run
        self.jm_thread = threading.Thread(target=start_jm,
                                          args=(self.jm, ADDR))

        self.addCleanup(self.cleanup)

    def test__setup(self):
        jm = jobmanager.JobManager(name='RuckusBringer')
        self.assertEqual(jm.name, 'RuckusBringer')

        self.assertFalse(jm.awaiting_startup_ack)
        self.assertEqual(jm.status, constants.STATUS.ready)

# EMQP Tests
    def test_reset(self):
        self.jm.reset()

        self.assertFalse(self.jm.awaiting_startup_ack)
        self.assertEqual(self.jm.status, constants.STATUS.ready)

    def test_start(self):
        sock = FakeDevice()

        self.jm_thread.start()
        time.sleep(.1)  # wait for the manager to warm up

        self.assertTrue(self.jm.awaiting_startup_ack)
        self.assertEqual(self.jm.status, constants.STATUS.connecting)

        # Give JM something to connect to.
        sock.zsocket.bind(ADDR)

        jm_addr, _, _, cmd, msgid, queues, type_ = sock.recv_multipart()
        self.assertEqual(self.jm.name, jm_addr)
        self.assertEqual(cmd, "INFORM")
        self.assertEqual(type_, constants.CLIENT_TYPE.worker)

        self.send_ack(sock, jm_addr, msgid)

        time.sleep(.1)
        self.assertEqual(self.jm.status, constants.STATUS.connected)

    def send_ack(self, sock, jm_addr, msgid):
        send_emqp_router_message(sock, jm_addr, "ACK", msgid)

    def test__start_event_loop(self):
        # Tests the first part of the event loop
        sock = FakeDevice()
        sock.zsocket.bind(ADDR)

        self.jm_thread.start()

        # Consume the INFORM command
        jm_addr, _, _, cmd, msgid, queues, type_ = sock.recv_multipart()
        self.send_ack(sock, jm_addr, msgid)

        # Test the correct number of READY messages is sent for the broker
        # to know how many jobs the JM can handle
        ready_msg_count = 0
        for i in range(0, conf.WORKERS):
            msg = sock.recv_multipart()
            if len(msg) > 4 and msg[3] == "READY":
                ready_msg_count += 1
        # If this fails, less READY messages were sent than were supposed
        # to be sent.
        self.assertEqual(ready_msg_count, conf.WORKERS)

    @unittest.skip('')
    def test_on_request(self):
        from ..client.messages import build_module_path
        sock = FakeDevice()
        sock.zsocket.bind(ADDR)
        self.jm_thread.start()

        jm_addr, _, _, _, msgid, _, _ = sock.recv_multipart()
        self.send_ack(sock, jm_addr, msgid)
        time.sleep(.1)  # give time for the JM to process

        path, callable_name = build_module_path(pretend_job)

        run_msg = ['run', {
            'path': path,
            'callable': callable_name,
        }]

        msg = (conf.DEFAULT_QUEUE_NAME, '', json.dumps(run_msg))

        send_emqp_router_message(sock, jm_addr, 'REQUEST', msg)

        self.assertFalse(self.jm.request_queue.empty())

    def cleanup(self):
        self.jm.on_disconnect(None, None)
        self.jm = None


def start_jm(jm, addr):
    jm.start(addr)


def pretend_job():
    time.sleep(1)
