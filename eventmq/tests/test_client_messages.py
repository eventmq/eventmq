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
import sys
import unittest

import mock
from testfixtures import LogCapture

from .. import conf
from ..client import messages


class TestClass(object):
    """
    class to use in the name_from_callable test
    """
    def mymethod(self):
        """
        this method is used as the callable in
        :meth:`TestCase.test_name_from_callable`
        """
        return True


class CallableTestClass(object):
    def __call__(self):
        return True


class TestCase(unittest.TestCase):
    @mock.patch('eventmq.client.messages.send_request')
    def test_defer_job(self, sndreq_mock):
        from future.moves.urllib.parse import urlsplit

        _msgid = 'mv029aisjf-09asdfualksd-aklds290fjoiw'

        sndreq_mock.return_value = _msgid
        socket = mock.Mock()

        msgid = messages.defer_job(socket, urlsplit,
                                   args=[1, 2],
                                   kwargs={'a': 1, 'b': 2},
                                   class_args=[9, 8],
                                   class_kwargs={'z': 9, 'y': 8},
                                   reply_requested=True,
                                   guarantee=False,
                                   retry_count=3,
                                   timeout=0,
                                   queue='test_queue')
        # defer_job should return _msgid untouched
        self.assertEqual(msgid, _msgid)

        if sys.version_info[0] == 3:
            msg = ['run', {
                'callable': 'urlsplit',
                'path': 'urllib.parse',
                'args': [1, 2],
                'kwargs': {'a': 1, 'b': 2},
                'class_args': [9, 8],
                'class_kwargs': {'z': 9, 'y': 8},
            }]
        else:
            msg = ['run', {
                'callable': 'urlsplit',
                'path': 'urlparse',
                'args': [1, 2],
                'kwargs': {'a': 1, 'b': 2},
                'class_args': [9, 8],
                'class_kwargs': {'z': 9, 'y': 8},
            }]

        sndreq_mock.assert_called_with(socket, msg,
                                       reply_requested=True,
                                       guarantee=False,
                                       retry_count=3,
                                       timeout=0,
                                       queue='test_queue')

        with LogCapture() as log_checker:
            # don't blow up on an invalid callable path
            messages.defer_job(socket, 'non-callable')

            # don't blow up on some random nameless func
            def nameless_func():
                return True
            nameless_func.func_name = ''
            messages.defer_job(socket, nameless_func)

            # don't blow-up for pathless functions
            nameless_func.func_name = 'nameless_func'
            nameless_func.__module__ = None
            messages.defer_job(socket, nameless_func)

            # log an error if a callable instance object is passed
            callable_obj = CallableTestClass()
            messages.defer_job(socket, callable_obj)

            log_checker.check(
                ('eventmq.client.messages',
                 'ERROR',
                 'Invalid callable string passed, absolute path required: "non-callable"'),  # noqa
                ('eventmq.utils.functions',
                 'ERROR',
                 'Encountered callable with no name in eventmq.tests.test_client_messages'),  # noqa
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered invalid callable, will not proceed.'),
                ('eventmq.utils.functions',
                 'ERROR',
                 'Encountered callable with no __module__ path nameless_func'),
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered invalid callable, will not proceed.'),
                ('eventmq.utils.functions',
                 'ERROR',
                 'Encountered unknown callable ({}) type instanceobject'.format(callable_obj)),  # noqa
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered invalid callable, will not proceed.'),
            )

    def test_name_from_callable(self):
        import json
        funcpath = messages.name_from_callable(json.dumps)

        t = TestClass()
        methpath = messages.name_from_callable(t.mymethod)

        self.assertEqual(funcpath, 'json.dumps')
        self.assertEqual(
            methpath,
            'eventmq.tests.test_client_messages:TestClass.mymethod')

    @mock.patch('eventmq.client.messages.send_schedule_request')
    def test_schedule(self, send_schedule_req_mock):
        import json
        _msgid = 'ovznopi4-)*(@#$Nn0av84-a0cn84n03'
        send_schedule_req_mock.return_value = _msgid

        socket = mock.Mock()

        msgid = messages.schedule(socket, json.dumps,
                                  interval_secs=500,
                                  args=(1, 2),
                                  kwargs={'a': 1, 'b': 2},
                                  class_args=(9, 8),
                                  class_kwargs={'z': 9, 'y': 8},
                                  headers=('guarantee', 'poop'),
                                  queue='blurp')

        # send_schedule should return an untouched message id
        self.assertEqual(msgid, _msgid)

        msg = ['run', {
            'callable': 'dumps',
            'path': 'json',
            'args': (1, 2),
            'kwargs': {'a': 1, 'b': 2},
            'class_args': (9, 8),
            'class_kwargs': {'z': 9, 'y': 8},
        }]

        send_schedule_req_mock.assert_called_with(
            socket,
            interval_secs=500,
            cron='',  # default arg
            message=msg,
            headers=('guarantee', 'poop'),
            queue='blurp',
            unschedule=False)  # default arg

        with LogCapture() as log_checker:
            # don't blow up on an invalid callable path
            messages.schedule(socket, 'non-callable',
                              class_args=(123,),
                              interval_secs=10)

            # don't blow up on some random nameless func
            def nameless_func():
                return True
            nameless_func.func_name = ''
            messages.schedule(socket, nameless_func,
                              class_args=(48,),
                              interval_secs=19)

            # don't blow-up for pathless functions
            nameless_func.func_name = 'nameless_func'
            nameless_func.__module__ = None
            messages.schedule(socket, nameless_func,
                              class_args=(123,),
                              interval_secs=8920)

            # log an error if a callable instance object is passed
            callable_obj = CallableTestClass()
            messages.schedule(socket, callable_obj,
                              class_args=(123,),
                              interval_secs=23)

            # error if neither cron or interval_secs is specified
            messages.schedule(socket, json.dumps, class_args=(123,))

            log_checker.check(
                ('eventmq.client.messages',
                 'ERROR',
                 'Invalid callable string passed, absolute path required: "non-callable"'),  # noqa
                ('eventmq.utils.functions',
                 'ERROR',
                 'Encountered callable with no name in eventmq.tests.test_client_messages'),  # noqa
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered invalid callable, will not proceed.'),
                ('eventmq.utils.functions',
                 'ERROR',
                 'Encountered callable with no __module__ path nameless_func'),
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered invalid callable, will not proceed.'),
                ('eventmq.utils.functions',
                 'ERROR',
                 'Encountered unknown callable ({}) type instanceobject'.format(callable_obj)),  # noqa
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered invalid callable, will not proceed.'),
                ('eventmq.client.messages',
                 'ERROR',
                 'You must sepcify either `interval_secs` or `cron`, but not both (or neither)'),  # noqa
            )

    @mock.patch('eventmq.client.messages.send_emqp_message')
    def test_send_request(self, snd_empq_msg_mock):
        _msgid = '0svny2rj8d0-aofinsud4839'
        snd_empq_msg_mock.return_value = _msgid

        socket = mock.Mock()

        msg = {'alksjfd': [1, 2],
               'laksdjf': 4,
               'alkfjds': 'alksdjf'}

        msgid = messages.send_request(socket, msg,
                                      reply_requested=True,
                                      guarantee=False,
                                      retry_count=2,
                                      queue='mozo')
        self.assertEqual(msgid, _msgid)
        snd_empq_msg_mock.assert_called_with(
            socket, 'REQUEST',
            ('mozo',
             'reply-requested,retry-count:2',
             messages.serialize(msg)))

    @mock.patch('eventmq.client.messages.send_emqp_message')
    def test_send_request_all_headers(self, snd_empq_msg_mock):
        _msgid = '0svny2rj8d0-aofinsud4839'
        snd_empq_msg_mock.return_value = _msgid

        socket = mock.Mock()

        msg = {'alksjfd': [1, 2],
               'laksdjf': 4,
               'alkfjds': 'alksdjf'}

        msgid = messages.send_request(socket, msg,
                                      reply_requested=True,
                                      guarantee=True,
                                      retry_count=2,
                                      timeout=3)
        self.assertEqual(msgid, _msgid)
        snd_empq_msg_mock.assert_called_with(
            socket, 'REQUEST',
            ('default',
             'reply-requested,guarantee,retry-count:2,timeout:3',
             messages.serialize(msg)))

    @mock.patch('eventmq.client.messages.send_emqp_message')
    def test_send_schedule_request(self, snd_empq_msg_mock):
        _msgid = 'va08n45-lanf548afn984-m7489vs'
        snd_empq_msg_mock.return_value = _msgid

        socket = mock.Mock()

        msg = {'20if': [1, 2],
               'mu8vc': 5,
               'zhx7': {'a': 1}}

        msgid = messages.send_schedule_request(socket, msg,
                                               interval_secs=38)
        self.assertEqual(msgid, _msgid)

        snd_empq_msg_mock.assert_called_with(
            socket, 'SCHEDULE',
            (conf.DEFAULT_QUEUE_NAME,
             '',
             '38',
             messages.serialize(msg),
             '')
        )

        msgid = messages.send_schedule_request(socket, msg,
                                               interval_secs=92,
                                               unschedule=True)
        self.assertEqual(msgid, _msgid)

        snd_empq_msg_mock.assert_called_with(
            socket, 'UNSCHEDULE',
            (conf.DEFAULT_QUEUE_NAME,
             '',
             '92',
             messages.serialize(msg),
             '')
        )
