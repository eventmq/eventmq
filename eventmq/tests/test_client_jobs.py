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

import mock
from testfixtures import LogCapture

from ..client import jobs


class TestCase(unittest.TestCase):
    BROKER_ADDR = 'tcp://127.0.0.1:3748'

    @mock.patch('eventmq.client.messages.defer_job')
    @mock.patch('eventmq.client.jobs.Sender')
    def test_job_decorator(self, Sender_mock, defer_job_mock):
        defer_job_mock.return_value = '43e14eaa-2034-4c84-8fe7-5577c70b6a7c'
        d = jobs.job(test_func, broker_addr=self.BROKER_ADDR)

        self.assertTrue(hasattr(d, 'delay'))

        self.assertEqual(d(), 12)
        self.assertEqual(d.delay(), '43e14eaa-2034-4c84-8fe7-5577c70b6a7c')
        defer_job_mock.assert_called_with(Sender_mock(), test_func, args=(),
                                          kwargs={}, queue=None)

        d = jobs.job(test_func, broker_addr=self.BROKER_ADDR, queue='mojo')
        d.delay(1, 2, three=3)
        defer_job_mock.assert_called_with(Sender_mock(), test_func,
                                          args=(1, 2), kwargs={'three': 3},
                                          queue='mojo')

    @mock.patch('eventmq.client.messages.defer_job')
    @mock.patch('eventmq.client.jobs.Sender')
    def test_job_decorator_no_broker(self, Sender_mock, defer_job_mock):
        defer_job_mock.return_value = '43e14eaa-2034-4c84-8fe7-5577c70b6a7c'
        d = jobs.job(test_func, queue='fut')

        with LogCapture() as log_checker:
            r = d.delay()

            log_checker.check(
                ('eventmq.client.jobs',
                 'WARNING',
                 'No EMQ_BROKER_ADDR defined. Running function `test_func` '
                 'synchronously'),)

        self.assertEqual(r, 12)

    def test_get_job_decorator(self):
        decorate = jobs.job(None)
        self.assertTrue(isinstance(decorate, jobs.Job))

        f = decorate(test_func)
        self.assertEqual(f(), 12)

    @mock.patch('eventmq.client.jobs.Sender')
    @mock.patch('eventmq.client.messages.schedule')
    def test_schedule_helper(self, sched_mock, Sender_mock):
        jobs.schedule(test_func, broker_addr=self.BROKER_ADDR,
                      interval_secs=20)

        sched_mock.assert_called_with(
            Sender_mock(), test_func, args=(), class_args=(),
            class_kwargs=None, cron=None, headers=('guarantee',),
            interval_secs=20, kwargs=None, queue='default')

    @mock.patch('eventmq.client.jobs.Sender')
    @mock.patch('eventmq.client.messages.schedule')
    def test_unschedule_helper(self, sched_mock, Sender_mock):
        jobs.unschedule(test_func, broker_addr=self.BROKER_ADDR)

        sched_mock.assert_called_with(
            Sender_mock(), test_func, args=(), class_args=(),
            class_kwargs=None, cron=None, headers=('guarantee',),
            interval_secs=None, kwargs=None, queue='default', unschedule=True)


def test_func():
    return 12
