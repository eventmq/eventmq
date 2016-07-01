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
import time
import unittest

import mock

from .. import conf, constants, jobmanager

ADDR = 'inproc://pour_the_rice_in_the_thing'


class TestCase(unittest.TestCase):
    def test__setup(self):
        jm = jobmanager.JobManager(name='RuckusBringer')
        self.assertEqual(jm.name, 'RuckusBringer')

        self.assertFalse(jm.awaiting_startup_ack)
        self.assertEqual(jm.status, constants.STATUS.ready)

# EMQP Tests
    def test_reset(self):
        jm = jobmanager.JobManager()

        self.assertFalse(jm.awaiting_startup_ack)
        self.assertEqual(jm.status, constants.STATUS.ready)

    @mock.patch('eventmq.jobmanager.sendmsg')
    def test_send_ready(self, sndmsg_mock):
        jm = jobmanager.JobManager()
        jm.send_ready()

        sndmsg_mock.assert_called_with(jm.outgoing, 'READY')

    @mock.patch('multiprocessing.pool.Pool.close')
    @mock.patch('eventmq.jobmanager.JobManager.process_message')
    @mock.patch('eventmq.jobmanager.Sender.recv_multipart')
    @mock.patch('eventmq.jobmanager.Poller.poll')
    @mock.patch('eventmq.jobmanager.JobManager.maybe_send_heartbeat')
    @mock.patch('eventmq.jobmanager.JobManager.send_ready')
    def test__start_event_loop(self, send_ready_mock, maybe_send_hb_mock,
                               poll_mock, sender_mock, process_msg_mock,
                               pool_close_mock):
        jm = jobmanager.JobManager()
        maybe_send_hb_mock.return_value = False
        poll_mock.return_value = {jm.outgoing: jobmanager.POLLIN}
        sender_mock.return_value = [1, 2, 3]

        jm._start_event_loop()

        # send int(conf.CONCURRENT_JOBS) ready messages
        self.assertEqual(conf.CONCURRENT_JOBS, send_ready_mock.call_count)

        process_msg_mock.assert_called_with(
            sender_mock.return_value)

        jm.received_disconnect = True
        jm._start_event_loop()
        self.assertTrue(pool_close_mock.called)

    @mock.patch('eventmq.jobmanager.worker.run')
    @mock.patch('multiprocessing.pool.Pool.apply_async')
    def test_on_request(self, apply_async_mock, run_mock):
        _msgid = 'aaa0j8-ac40jf0-04tjv'
        _msg = ['a', 'b', '["run", {"a": 1}]']

        jm = jobmanager.JobManager()

        jm.on_request(_msgid, _msg)
        apply_async_mock.assert_called_with(
            args=({'a': 1}, _msgid),
            callback=jm.worker_done,
            func=run_mock)

    @mock.patch('zmq.Socket.unbind')
    def test_on_disconnect(self, socket_mock):
        msgid = 'goog8l-uitty40-007b'
        msg = ['a', 'b', 'whatever']

        socket_mock.return_value = True

        jm = jobmanager.JobManager()
        jm.outgoing.status = constants.STATUS.listening
        jm.on_disconnect(msgid, msg)
        self.assertTrue(jm.received_disconnect, "Did not receive disconnect.")

    @mock.patch('eventmq.jobmanager.JobManager.send_ready')
    @mock.patch('multiprocessing.pool.Pool.apply_async')
    def test_active_job_counts(self, apply_async_mock, send_ready_mock):
        msgids = ('goog8l-uitty40-007b','aaa0j8-ac40jf0-04tjv',
                 'a3jd90-yte3c00-3dfxw', 'bcvyej1-3sdfxv-34dsf',
                 '23aax3-abc342-ccc3d', 'uitty40-ac40jf0-003bx')
        msg = ['a', 'b', '["run", {"a": 1}]']

        jm = jobmanager.JobManager()
        apply_async_mock.side_effects = [x for x in range(0,6)]
        send_ready_mock.return_value = True
        for msgid in msgids:
            jm.on_request(msgid, msg)
            self.assertTrue(jm.job_slots > jm.available_workers)
            pretend_job()
            jm.worker_done(msgid)
            self.assertTrue(jm.job_slots == jm.available_workers)
        self.assertTrue(jm.job_slots == jm.available_workers)

    # Other Tests
    @mock.patch('eventmq.jobmanager.JobManager.start')
    @mock.patch('eventmq.jobmanager.import_settings')
    @mock.patch('eventmq.jobmanager.Sender.rebuild')
    def test_sighup_handler(self, rebuild_mock, import_settings_mock,
                            start_mock):
        jm = jobmanager.JobManager()

        jm.sighup_handler(982374, "FRAMEY the frame")

        self.assertTrue(rebuild_mock.called)

        # called once for the default settings, once for the jobmanager
        # settings
        self.assertEqual(2, import_settings_mock.call_count)
        # check to see if the last call was called with the jobmanager section
        import_settings_mock.assert_called_with(section='jobmanager')

        start_mock.assert_called_with(
            addr=conf.WORKER_ADDR,
        )

    @mock.patch('eventmq.jobmanager.JobManager.start')
    @mock.patch('eventmq.jobmanager.import_settings')
    @mock.patch('eventmq.jobmanager.setup_logger')
    def test_jobmanager_main(self, setup_logger_mock, import_settings_mock,
                             start_mock):
        jm = jobmanager.JobManager()

        jm.jobmanager_main()

        setup_logger_mock.assert_called_with('')
        self.assertEqual(2, import_settings_mock.call_count)
        # Assert that the last call to import settings was for the jobmanager
        # section
        import_settings_mock.assert_called_with(section='jobmanager')

        start_mock.assert_called_with(addr=conf.WORKER_ADDR,
                                      queues=conf.QUEUES)

        jm.queues = ((10, 'derp'), (0, 'blurp'))
        jm.jobmanager_main()

        start_mock.assert_called_with(addr=conf.WORKER_ADDR,
                                      queues=jm.queues)

    def cleanup(self):
        self.jm.on_disconnect(None, None)
        self.jm = None


def call_done(jm):
    jm.active_jobs += 1
    return True


def start_jm(jm, addr):
    jm.start(addr)


def pretend_job():
    time.sleep(1)
