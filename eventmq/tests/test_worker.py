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
import logging

import mock
import json
import time

from .. import jobmanager
from .. import router

from tl.testing.thread import ThreadAwareTestCase

BACKEND_ADDR = 'inproc://kodak_film_festivals'
FRONTEND_ADDR = 'inproc://frontend'


logger = logging.getLogger(__name__)


class TestCase(ThreadAwareTestCase):
    def setUp(self):
        self.jobmanager = jobmanager.JobManager()
        self.router = router.Router()

        self.thread = threading.Thread(target=start_router,
                                       args=(self.router,))

        self.thread2 = threading.Thread(target=start_jobmanager,
                                        args=(self.jobmanager,))

        self.addCleanup(self.cleanup)

    @mock.patch('signal.signal')
    def test_start(self, mock_signal_signal):
        self.thread.start()
        self.thread2.start()
        time.sleep(1)

        msg = ['run', {
            'callable': 'test_job',
            'path': 'eventmq.scheduler',
            'args': None,
            'kwargs': {},
            'class_args': None,
            'class_kwargs': {},
        }]
        time.sleep(2)

        full_msg = ['REQUEST', 'default', json.dumps(msg)]
        self.jobmanager.on_request(msgid='1234',
                                   msg=full_msg)

    def cleanup(self):
        self.jobmanager.on_disconnect(None, None)
        self.router.on_disconnect(None, None)
        return


def start_router(router):
    router.start(FRONTEND_ADDR, BACKEND_ADDR)
    time.sleep(2)


def start_jobmanager(jobmanager):
    jobmanager.jobmanager_main(addr=BACKEND_ADDR)
    time.sleep(2)
