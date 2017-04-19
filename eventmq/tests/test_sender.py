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

import zmq

from .. import sender


class TestCase(unittest.TestCase):
    def setUp(self):
        self.zcontext = zmq.Context.instance()

        self.sender = sender.Sender()

    def test_send_multipart(self):
        # Test that multipart installs the correct headers
        socket = self.zcontext.socket(zmq.ROUTER)
        socket.bind('inproc://test_send_multipart')
        self.sender.connect('inproc://test_send_multipart')

        self.sender.send_multipart(('Hello!', ), '1')

        self.assert_(socket.poll() != 0)
        msg = socket.recv_multipart()
        self.assertEqual(msg[0].decode('ascii'), self.sender.name)
        self.assertEqual(msg[1], b'')
        self.assertEqual(msg[2], b'1')
        self.assertEqual(msg[3], b'Hello!')

    def test_send_multipart_unicode(self):
        # Test that send_multipart handles unicode safely
        pass

    def test_listen(self):
        # test that the listen method functions correctly
        with self.assertRaises(sender.exceptions.EventMQError):
            self.sender.status = 'something else'
            self.sender.listen('ipc://emq-test_sender.ipc')

        # put the status back
        self.sender.status = sender.constants.STATUS.ready
        self.sender.listen('ipc://emq-test_sender.ipc')
        self.assertEqual(self.sender.status, sender.constants.STATUS.listening)

    def test_connect(self):
        # test that the connect method functions correctly
        with self.assertRaises(sender.exceptions.EventMQError):
            self.sender.status = 'something else'
            self.sender.connect()

        self.sender.status = sender.constants.STATUS.ready
        self.sender.connect('ipc://emq-test_sender.ipc')

    def test_disconnect(self):
        with self.assertRaises(Exception):
            self.sender.status = sender.constants.STATUS.ready
            self.sender.listen('ipc://emq-test_sender.ipc')
            self.sender.status = 'bogus'
            self.sender.unbind('ipc://emq-test_sender.ipc')

        self.sender.status = sender.constants.STATUS.ready
        self.sender.listen('ipc://emq-test_sender.ipc')
        self.sender.unbind('ipc://emq-test_sender.ipc')

    def test_rebuild(self):
        self.sender.listen('ipc://emq-test_sender.ipc')
        self.assertEqual(self.sender.status, sender.constants.STATUS.listening)

        self.sender.rebuild()
        self.assertEqual(self.sender.status, sender.constants.STATUS.ready)
