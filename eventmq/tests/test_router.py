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
import threading
import unittest

import mock

from .. import router

ADDR = 'inproc://kodak_film_festival'


class TestCase(unittest.TestCase):
    def setUp(self):
        self.router = router.Router()

        self.thread = threading.Thread(target=start_router,
                                       args=(self.router,))

        self.addCleanup(self.cleanup)

    @mock.patch('signal.signal')
    def test_start(self, mock_signal_signal):
        self.thread.start()

    def cleanup(self):
        self.router.on_disconnect(None, None)

def start_router(router):
    router.start(ADDR)
