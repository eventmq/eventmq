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

import logging
import sys
import time

import mock

from .. import worker

if sys.version[0] == '2':
    import Queue
else:
    import queue as Queue


ADDR = 'inproc://pour_the_rice_in_the_thing'
SETUP_SUCCESS_RETVAL = 'job setup success'


def test_run_with_timeout():
    payload = {
        'path': 'eventmq.tests.test_worker',
        'callable': 'job',
        'args': [2]
    }

    msgid = worker._run_job(payload, logging.getLogger())

    assert msgid


@mock.patch('eventmq.worker.callable_from_name')
def test_run_job_setup_hook(callable_from_name_mock):
    from eventmq import conf

    setup_func_str = 'eventmq.tests.test_worker.job_setup_hook'
    callable_from_name_mock.return_value = mock.Mock()

    payload = {
        'path': 'eventmq.tests.test_worker',
        'callable': 'job',
        'args': [2]
    }

    q, res_q = Queue.Queue(), Queue.Queue()

    q.put(payload)
    q.put('DONE')

    try:
        conf.JOB_ENTRY_FUNC = setup_func_str
        worker._run(q, res_q, logging.getLogger())
    finally:
        conf.JOB_ENTRY_FUNC = ''

    callable_from_name_mock.assert_called_with(setup_func_str)
    assert callable_from_name_mock.return_value.call_count == 1


@mock.patch('eventmq.worker.callable_from_name')
def test_run_job_teardown_hook(callable_from_name_mock):
    from eventmq import conf

    teardown_func_str = 'eventmq.tests.test_worker.job_teardown_hook'
    callable_from_name_mock.return_value = mock.Mock()

    payload = {
        'path': 'eventmq.tests.test_worker',
        'callable': 'job',
        'args': [2]
    }

    q, res_q = Queue.Queue(), Queue.Queue()

    q.put(payload)
    q.put('DONE')

    try:
        conf.JOB_EXIT_FUNC = teardown_func_str
        worker._run(q, res_q, logging.getLogger())
    finally:
        conf.JOB_EXIT_FUNC = ''

    callable_from_name_mock.assert_called_with(teardown_func_str)
    assert callable_from_name_mock.return_value.call_count == 1


@mock.patch('eventmq.worker.callable_from_name')
def test_run_subprocess_setup_func(callable_from_name_mock):
    from eventmq import conf

    setup_func_str = 'eventmq.tests.test_worker.process_setup_hook'
    callable_from_name_mock.return_value = mock.Mock()

    payload = {
        'path': 'eventmq.tests.test_worker',
        'callable': 'job',
        'args': [2]
    }

    q, res_q = Queue.Queue(), Queue.Queue()

    q.put(payload)
    q.put('DONE')

    try:
        conf.SUBPROCESS_SETUP_FUNC = setup_func_str
        worker._run(q, res_q, logging.getLogger())
    finally:
        conf.SUBPROCESS_SETUP_FUNC = ''

    callable_from_name_mock.assert_called_with(setup_func_str)
    assert callable_from_name_mock.return_value.call_count == 1


@mock.patch('eventmq.worker.run_setup')
def test_run_run_setup_func(run_setup_mock):
    from eventmq import conf

    setup_func_path = 'eventmq.tests.test_worker'
    setup_func_callable = 'process_setup_hook'

    payload = {
        'path': 'eventmq.tests.test_worker',
        'callable': 'job',
        'args': [2]
    }

    q, res_q = Queue.Queue(), Queue.Queue()

    q.put(payload)
    q.put('DONE')

    try:
        conf.SETUP_PATH = setup_func_path
        conf.SETUP_CALLABLE = setup_func_callable
        worker._run(q, res_q, logging.getLogger())
    finally:
        conf.SETUP_PATH = ''
        conf.SETUP_CALLABLE = ''

    run_setup_mock.assert_called_with(setup_func_path, setup_func_callable)


def test_run_setup():
    setup_callable = 'process_setup_hook'
    setup_path = 'eventmq.tests.test_worker'

    assert worker.run_setup(setup_path, setup_callable)


def job(sleep_time=0):
    time.sleep(sleep_time)

    return True


def process_setup_hook():
    return True


def job_setup_hook():
    return SETUP_SUCCESS_RETVAL


def job_teardown_hook():
    return True
