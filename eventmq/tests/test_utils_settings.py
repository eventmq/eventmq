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

from testfixtures import LogCapture

from . import utils  # test utilities
from .. import settings
from ..settings import conf


class TestCase(unittest.TestCase):
    settings_ini = "\n".join(
        ("[global]",
         "super_debug=FaLsE",
         "hide_heartbeat_logs=false",
         "frontend_listen_addr=tcp://1.2.3.4:1234",
         "",
         "[jobmanager]",
         "hide_heartbeat_logs=true",
         "listen_addr=tcp://160.254.23.88:47290",
         'queues=[[50,"google"], [40,"pushes"], [10,"default"]]',
         "concurrent_jobs=9283",
         "",
         "[router]",
         "# Listen addresses",
         "frontend_listen_addr=tcp://123.211.1.1:47291",
         "backend_listen_addr=tcp://123.211.1.1:47290",
         "# Log all messages",
         "super_dEbug=TrUe",
         ""))

    def test_load_settings_default(self):
        # Test loading the global section only
        with LogCapture() as log_checker:
            with utils.mock_config_file(self.settings_ini):
                settings.load_settings_from_file()

            log_checker.check(
                ('eventmq.settings',
                 'DEBUG',
                 'Setting conf.SUPER_DEBUG to False'),
                ('eventmq.settings',
                 'DEBUG',
                 'Setting conf.HIDE_HEARTBEAT_LOGS to False'),
                ('eventmq.settings',
                 'WARNING',
                 'Tried to set invalid setting: frontend_listen_addr='
                 'tcp://1.2.3.4:1234'))

        # Defined in the global section
        self.assertFalse(conf.SUPER_DEBUG)
        self.assertFalse(conf.HIDE_HEARTBEAT_LOGS)

        # Other settings shouldn't be defined
        with self.assertRaises(AttributeError):
            conf.CONCURRENT_JOBS

        with self.assertRaises(AttributeError):
            conf.frontend_listen_addr

    def test_read_section(self):
        # Test reading the router section
        with utils.mock_config_file(self.settings_ini):
            settings.load_settings_from_file('router')

        # Changed in global section
        self.assertFalse(conf.HIDE_HEARTBEAT_LOGS)

        # Changed in router section
        self.assertEqual(conf.FRONTEND_LISTEN_ADDR, 'tcp://123.211.1.1:47291')
        self.assertEqual(conf.BACKEND_LISTEN_ADDR, 'tcp://123.211.1.1:47290')
        self.assertTrue(conf.SUPER_DEBUG)

        # # Changed
        # self.assertEqual(conf.QUEUES,
        #                  [(50, 'google'), (40, 'pushes'), (10, 'default')])

        # self.assertEqual(conf.CONNECT_ADDR, 'tcp://160.254.23.88:47290')

    def test_invalid_section(self):
        conf.CONCURRENT_JOBS = 1234
        conf.QUEUES = [(50, 'google'), (40, 'pushes'), (10, 'default')]
        conf.CONNECT_ADDR = 'tcp://160.254.23.88:47290'

        # Invalid section
        with utils.mock_config_file(self.settings_ini):
            self.assertRaises(
                ValueError,
                settings.load_settings_from_file, 'nonexistent_section')

    def test_invalid_json(self):
        settings_ini = "\n".join(
            ("[global]",
             "super_debug=TRuE",
             "frontend_addr=tcp://0.0.0.0:47291",
             "",
             "[jobmanager]",
             "super_debug=FalSe",
             'queues=[[50,google], [40,"pushes"], [10,"default"]]',
             "worker_addr=tcp://160.254.23.88:47290",
             "concurrent_jobs=9283",))
        # Ensure fresh test data
        conf.reload()

        with utils.mock_config_file(settings_ini):
            self.assertRaises(ValueError,
                              settings.load_settings_from_file, 'jobmanager')

    def test_parse_string_array(self):
        # Tests parsing a non-nested array (nested are tested via QUEUES
        # setting) via FAKE_VALUE
        settings_ini = "\n".join(
            ("[jobmanager]",
             'fake_value=["asdf", "asdf2"]',))
        # Ensure fresh test data
        conf.reload()
        conf.FAKE_VALUE = [u'pew', u'pew']

        with utils.mock_config_file(settings_ini):
            settings.load_settings_from_file('jobmanager')

        self.assertEqual(conf.FAKE_VALUE, [u'asdf', u'asdf2'])

    def test_parse_dict_array(self):
        # Tests parsing an array of dictionaries
        settings_ini = '\n'.join(
            ("[jobmanager]",
             'fake_value=[{"key1": "value1"}, {"key2": "value2"}]')
        )
        conf.reload()
        conf.FAKE_VALUE = [{'default': 1}]
        with utils.mock_config_file(settings_ini):
            settings.load_settings_from_file('jobmanager')

        self.assertEqual(conf.FAKE_VALUE,
                         [{u"key1": u"value1"}, {u"key2": u"value2"}])

    def test_invalid_setting(self):
        settings_ini = "\n".join(
            ("[global]",
             'nonexistent_setting=rabbit blood',))
        # Ensure fresh test data
        conf.reload()

        with LogCapture() as log_checker:
            with utils.mock_config_file(settings_ini):
                settings.load_settings_from_file()

            log_checker.check(
                ('eventmq.settings',
                 'WARNING',
                 'Tried to set invalid setting: nonexistent_setting=rabbit '
                 'blood'))
