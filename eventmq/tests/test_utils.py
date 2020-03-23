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

#

# ConfigParser was renamed to configparser in python 3. Do this try...except
# to maintain python 2/3 compatability
try:
    from configparser import ConfigParser
except ImportError:
    import ConfigParser

from imp import reload
import io
import os
import random
import sys
import unittest

import mock
from six.moves import range

from .. import conf
from .. import constants
from .. import exceptions
from ..utils import classes, messages, settings


class SettingsTestCase(unittest.TestCase):
    settings_ini = '\n'.join(
        ('[global]',
         'super_debug=TRuE',
         'frontend_addr=tcp://0.0.0.0:47291',
         '',
         '[jobmanager]',
         'super_debug=FalSe',
         'queues=[[50,"google"], [40,"pushes"], [10,"default"]]',
         'worker_addr=tcp://160.254.23.88:47290',
         'concurrent_jobs=9283',
         '',
         '[scheduler]',
         'redis_client_class_kwargs={"test_kwarg": true}',
         '',
         '[section_with_bad_list]',
         'queues=[[10,asdf],]',
         '',
         '[section_with_bad_dict]',
         'redis_client_class_kwargs={asdf, 39}'))

    def setUp(self):
        self._config = ConfigParser()

        if sys.version_info[0] == 3:
            self._config.read_string(self.settings_ini)
        else:
            self._config.readfp(io.BytesIO(self.settings_ini))

        # sometimes the tests step on each other with this module. reloading
        # ensures fresh test data
        reload(conf)

    @mock.patch('eventmq.utils.settings.os.path.exists')
    def test_import_settings_default(self, pathexists_mock):
        pathexists_mock.return_value = True

        with mock.patch('eventmq.utils.settings.ConfigParser',
                        return_value=self._config):
            with mock.patch.object(self._config, 'read'):
                settings.import_settings()

        # Changed. Default is false
        self.assertTrue(conf.SUPER_DEBUG, True)

        # Default True
        self.assertTrue(conf.HIDE_HEARTBEAT_LOGS)

        # Default is 4
        self.assertEqual(conf.CONCURRENT_JOBS, 4)

        # Changed. Default is 127.0.0.1:47291
        self.assertEqual(conf.FRONTEND_ADDR, 'tcp://0.0.0.0:47291')

        # Default is (10, 'default')
        self.assertEqual(conf.QUEUES, [(10, conf.DEFAULT_QUEUE_NAME), ])

    @mock.patch('eventmq.utils.settings.os.path.exists')
    def test_import_settings_jobmanager(self, pathexists_mock):
        pathexists_mock.return_value = True

        with mock.patch('eventmq.utils.settings.ConfigParser',
                        return_value=self._config):
            with mock.patch.object(self._config, 'read'):
                settings.import_settings()
                settings.import_settings('jobmanager')

        # Changed from True (in global) to False
        self.assertFalse(conf.SUPER_DEBUG)
        # Override default value
        self.assertEqual(conf.CONCURRENT_JOBS, 9283)

        # Changed
        self.assertEqual(conf.QUEUES,
                         [(50, 'google'), (40, 'pushes'), (10, 'default')])

        self.assertEqual(conf.WORKER_ADDR, 'tcp://160.254.23.88:47290')

    @mock.patch('eventmq.utils.settings.os.path.exists')
    def test_load_invalid_section_uses_defaults(self, pathexists_mock):
        pathexists_mock.return_value = True

        with mock.patch('eventmq.utils.settings.ConfigParser',
                        return_value=self._config):
            with mock.patch.object(self._config, 'read'):
                settings.import_settings('jobmanager')
                settings.import_settings('nonexistent_section')

        self.assertEqual(conf.CONCURRENT_JOBS, 9283)
        self.assertEqual(conf.QUEUES,
                         [(50, 'google'), (40, 'pushes'), (10, 'default')])
        self.assertEqual(conf.WORKER_ADDR, 'tcp://160.254.23.88:47290')

    @mock.patch('eventmq.utils.settings.os.path.exists')
    def test_dictionary(self, pathexists_mock):
        pathexists_mock.return_value = True

        with mock.patch('eventmq.utils.settings.ConfigParser',
                        return_value=self._config):
            with mock.patch.object(self._config, 'read'):
                settings.import_settings('scheduler')

        # Dictionary should be dictionary
        self.assertTrue(isinstance(conf.REDIS_CLIENT_CLASS_KWARGS, dict))
        # Dictionary should be dictionary
        self.assertTrue(conf.REDIS_CLIENT_CLASS_KWARGS['test_kwarg'])

    @mock.patch('eventmq.utils.settings.os.path.exists')
    def test_favor_environment_variables(self, pathexists_mock):
        pathexists_mock.return_value = True

        # frontend_addr has been defined in global, but should be the following
        # value
        value = 'from environment variable'
        os.environ['EVENTMQ_FRONTEND_ADDR'] = value
        try:
            with mock.patch('eventmq.utils.settings.ConfigParser',
                            return_value=self._config):
                with mock.patch.object(self._config, 'read'):
                    settings.import_settings()
        finally:
            del os.environ['EVENTMQ_FRONTEND_ADDR']

        self.assertEqual(conf.FRONTEND_ADDR, value)

    @mock.patch('eventmq.utils.settings.os.path.exists')
    def test_valueerror_on_invalid_json_list(self, pathexists_mock):
        pathexists_mock.return_value = True

        with mock.patch('eventmq.utils.settings.ConfigParser',
                        return_value=self._config):
            with mock.patch.object(self._config, 'read'):
                with self.assertRaises(ValueError):
                    settings.import_settings('section_with_bad_list')

    @mock.patch('eventmq.utils.settings.os.path.exists')
    def test_valueerror_on_invalid_json_dict(self, pathexists_mock):
        pathexists_mock.return_value = True

        with mock.patch('eventmq.utils.settings.ConfigParser',
                        return_value=self._config):
            with mock.patch.object(self._config, 'read'):
                with self.assertRaises(ValueError):
                    settings.import_settings('section_with_bad_dict')


class EMQPServiceTestCase(unittest.TestCase):

    # pretend to be an emq socket
    outgoing = 'some-outgoing-socket'

    def get_worker(self):
        """return an EMQPService mimicking a worker"""
        obj = classes.EMQPService()
        obj.SERVICE_TYPE = constants.CLIENT_TYPE.worker
        obj.outgoing = self.outgoing
        obj._meta = {
            'last_sent_heartbeat': 0
        }

        return obj

    def get_scheduler(self):
        """return an EMQPService mimicking a scheduler"""
        obj = classes.EMQPService()
        obj.SERVICE_TYPE = constants.CLIENT_TYPE.scheduler
        obj.outgoing = self.outgoing
        obj._meta = {
            'last_sent_heartbeat': 0
        }

        return obj

    @mock.patch('eventmq.utils.classes.sendmsg')
    def test_send_inform_return_msgid(self, sendmsg_mock):
        obj = self.get_worker()
        sendmsg_mock.return_value = 'some-msgid'

        retval = obj.send_inform(queues=[(10, 'default'), ])

        self.assertEqual(retval, sendmsg_mock.return_value)

    @mock.patch('eventmq.utils.classes.sendmsg')
    def test_send_inform_empty_queue_name(self, sendmsg_mock):
        obj = self.get_worker()

        obj.send_inform()

        sendmsg_mock.assert_called_with(
            self.outgoing, 'INFORM',
            ['', constants.CLIENT_TYPE.worker])

    @mock.patch('eventmq.utils.classes.sendmsg')
    def test_send_inform_scheduler(self, sendmsg_mock):
        obj = self.get_scheduler()

        obj.send_inform(queues="this shouldn't matter")

        sendmsg_mock.assert_called_with(
            self.outgoing, 'INFORM',
            ['', constants.CLIENT_TYPE.scheduler]
        )

    @mock.patch('eventmq.utils.classes.sendmsg')
    def test_send_inform_specified_valid_queues(self, sendmsg_mock):
        obj = self.get_worker()

        obj.send_inform(queues=([10, 'push'], [7, 'email'],
                                [3, 'default']))
        sendmsg_mock.assert_called_with(
            'some-outgoing-socket', 'INFORM',
            ['[[10, "push"], [7, "email"], [3, "default"]]',
             constants.CLIENT_TYPE.worker]
        )

    @mock.patch('eventmq.utils.classes.sendmsg')
    def test_send_inform_update_last_sent_heartbeat(self, sendmsg_mock):
        obj = self.get_worker()

        obj.send_inform(queues=(['', constants.CLIENT_TYPE.worker]))

        self.assertGreater(obj._meta['last_sent_heartbeat'], 0)


class TestCase(unittest.TestCase):
    def test_generate_msgid(self):
        msgid = messages.generate_msgid()

        self.assertEqual(type(msgid), str)

    def test_parse_message(self):
        emq_headers = ('myid', '', 'protoversion', 'command', 'msgid')
        emq_frame_singlemsg = emq_headers + ('my message',)
        emq_frame_manymsg = emq_headers + ('many', 'parts')
        emq_frame_nomsg = emq_headers

        singlemsg = messages.parse_router_message(emq_frame_singlemsg)
        manymsg = messages.parse_router_message(emq_frame_manymsg)
        nomsg = messages.parse_router_message(emq_frame_nomsg)

        self.assertEqual(singlemsg[0], emq_frame_singlemsg[0])
        self.assertEqual(singlemsg[1], emq_frame_singlemsg[3])
        self.assertEqual(singlemsg[2], emq_frame_singlemsg[4])
        self.assertEqual(singlemsg[3], (emq_frame_singlemsg[5],))

        self.assertEqual(manymsg[0], emq_frame_manymsg[0])
        self.assertEqual(manymsg[1], emq_frame_manymsg[3])
        self.assertEqual(manymsg[2], emq_frame_manymsg[4])
        self.assertEqual(manymsg[3], emq_frame_manymsg[5:])

        self.assertEqual(nomsg[0], emq_frame_nomsg[0])
        self.assertEqual(nomsg[1], emq_frame_nomsg[3])
        self.assertEqual(nomsg[2], emq_frame_nomsg[4])
        self.assertEqual(nomsg[3], ())

        broken_message = ('dlkajfs', 'lkasdjf')
        with self.assertRaises(exceptions.InvalidMessageError):
            messages.parse_router_message(broken_message)

    def test_emqDeque(self):

        full = random.randint(1, 100)
        pfull = random.randint(1, full-1)

        q = classes.EMQdeque(full=full,
                             pfull=pfull)

        # P Fill
        for i in range(0, pfull):
            q.append(i)
        self.assertTrue(q.is_pfull())
        self.assertFalse(q.is_full())

        while len(q) > 0:
            q.pop()

        # Fill
        for i in range(0, full*4):
            q.append(i)

        self.assertTrue(q.is_full())
        self.assertTrue(q.is_pfull())

        # Is iterable?
        for i in q:
            assert True

        # Check overflow
        self.assertEqual(len(q), full)

        # Remove everything we tried to insert
        for i in range(0, full*4):
            if i in q:
                q.remove(i)
            elif len(q) > 0:
                q.popleft()

        # Assert empty
        self.assertEqual(len(q), 0)
        self.assertTrue(q.is_empty())

        for i in q:
            q.remove(i)

        self.assertEqual(len(q), 0)
