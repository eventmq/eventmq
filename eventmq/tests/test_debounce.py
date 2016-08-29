import uuid
import unittest
import mock
import redis

from .. import conf

from eventmq.client import messages


def test_job_function():
    return 'test_job_function'


class TestCase(unittest.TestCase):
    def setUp(self):
        super(TestCase, self).setUp()
        self.redis = redis.StrictRedis(
            host=conf.RQ_HOST,
            port=conf.RQ_PORT,
            db=conf.RQ_DB,
            password=conf.RQ_PASSWORD)
        self.socket = mock.Mock()

        self.job_id = str(uuid.uuid4())
        self.arguments_hash_mock = mock.patch(
            'eventmq.utils.functions.arguments_hash', lambda *x: self.job_id)
        self.arguments_hash_mock.start()

    def tearDown(self):
        cache_key1 = '{}_key1'.format(self.job_id)
        cache_key2 = '{}_key2'.format(self.job_id)
        self.redis.delete(cache_key1)
        self.redis.delete(cache_key2)

        self.arguments_hash_mock.stop()

        super(TestCase, self).tearDown()

    @mock.patch('eventmq.client.debounce._debounce_schedule')
    def test_debounce_initial_call(self, debounce_schedule_mock):
        messages.defer_job(
            self.socket,
            test_job_function,
            debounce_secs=1)

        debounce_schedule_mock.assert_called_with(
            path='eventmq.tests.test_debounce',
            callable_name='test_job_function',
            args=(),
            kwargs={},
            class_args=(),
            class_kwargs={},
            queue='default',
            reply_requested=False,
            retry_count=0,
            guarantee=False,
            debounce_secs=1,
            socket=self.socket)

    @mock.patch('eventmq.client.debounce._debounce_run_deferred_job')
    def test_debounce_step1(self, run_deferred_job_mock):
        """Should call through to _debounce_run_deferred_job."""
        from eventmq.client import debounce
        from eventmq.utils.functions import path_from_callable

        path, callable_name = path_from_callable(test_job_function)

        context = {
            'job_id': self.job_id,
            'cache_key1': '{}_key1'.format(self.job_id),
            'cache_key2': '{}_key2'.format(self.job_id),
            'path': path,
            'callable_name': callable_name,
            'queue': 'default',
            'debounce_secs': 1,
            'scheduled': False,
        }

        debounce._debounce_deferred_job(context)

        context.pop('debounce_secs', None)

        run_deferred_job_mock.assert_called_with(context)

    @mock.patch('eventmq.sender.Sender')
    @mock.patch('eventmq.client.debounce._debounce_run_deferred_job')
    @mock.patch('eventmq.client.messages.schedule')
    def test_debounce_step2(self, schedule_mock, run_deferred_job_mock,
                            sender_mock):
        """Should call through to _debounce_run_deferred_job."""
        from eventmq.client import debounce
        from eventmq.utils.functions import path_from_callable

        sender_mock.return_value = self.socket

        path, callable_name = path_from_callable(test_job_function)

        context = {
            'job_id': self.job_id,
            'cache_key1': '{}_key1'.format(self.job_id),
            'cache_key2': '{}_key2'.format(self.job_id),
            'path': path,
            'callable_name': callable_name,
            'queue': 'default',
            'debounce_secs': 1,
            'scheduled': False,
        }

        debounce._debounce_deferred_job(context)

        context.pop('debounce_secs', None)

        run_deferred_job_mock.assert_called_with(context)
        run_deferred_job_mock.reset_mock()

        # this time, since we already went through step1, it should schedule a
        # job for step2
        context['debounce_secs'] = 1
        debounce._debounce_deferred_job(context)

        run_deferred_job_mock.assert_not_called()

        context['scheduled'] = True
        context.pop('debounce_secs', None)

        schedule_mock.assert_called_with(
            socket=self.socket,
            func=run_deferred_job_mock,
            headers=('nohaste', 'guarantee'),
            class_args=(self.job_id,),
            interval_secs=1,
            kwargs={'context': context},
            queue='default')
