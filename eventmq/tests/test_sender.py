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
import unittest

import zmq

from .. import sender


class TestCase(unittest.TestCase):
    def setUp(self):
        self.zcontext = zmq.Context.instance()

        self.sender = sender.Sender()

    def test_send_multipart(self):
        # Test that multipart installs the correct headers
        socket = self.zcontext.socket(zmq.DEALER)
        socket.bind('inproc://test_send_multipart')
        self.sender.connect('inproc://test_send_multipart')

        self.sender.send_multipart(('Hello!', ), '1')

        self.assert_(socket.poll() != 0)
        msg = socket.recv_multipart()
        self.assertEqual(msg[0], self.sender.name)
        self.assertEqual(msg[1], '')
        self.assertEqual(msg[2], '1')
        self.assertEqual(msg[3], 'Hello!')

    def test_send_multipart_unicode(self):
        # Test that send_multipart handles unicode safely
        pass
