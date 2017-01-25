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

from .. import receiver, router, sender


class TestCase(unittest.TestCase):
    def setUp(self):
        self.zcontext = zmq.Context.instance()

        self.router = router.Router()
        self.receiver = self.router.incoming
        self.sender = sender.Sender()

    def test_send_multipart_unicode(self):
        # Test that send_multipart handles unicode safely
        pass

    def test_disconnect(self):
        with self.assertRaises(Exception):
            self.receiver.status = receiver.constants.STATUS.ready
            self.receiver.listen('ipc://emq-test_receiver.ipc')
            self.receiver.status = 'bogus'
            self.router.sighup_handler()

        self.receiver.status = receiver.constants.STATUS.ready
        self.receiver.listen('ipc://emq-test_receiver.ipc')
        self.receiver.unbind('ipc://emq-test_receiver.ipc')
