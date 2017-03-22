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
from imp import reload
import unittest

from testfixtures import LogCapture

from . import utils  # test utilities
from .. import conf
from ..utils import settings


class TestCase(unittest.TestCase):
    settings_ini = "\n".join(
        ("[global]",
         "super_debug=TRuE",
         "frontend_addr=tcp://0.0.0.0:47291",
         "",
         "[jobmanager]",
         "super_debug=FalSe",
         'queues=[[50,"google"], [40,"pushes"], [10,"default"]]',
         "worker_addr=tcp://160.254.23.88:47290",
         "concurrent_jobs=9283",))

    def test_import_settings_default(self):
        # sometimes the tests step on each other with this module. reloading
        # ensures fresh test data
        reload(conf)

        # Global section
        # --------------
        with utils.mock_config_file(self.settings_ini):
            settings.import_settings()

        # Changed. Default is 127.0.0.1:47291
        self.assertEqual(conf.FRONTEND_ADDR, 'tcp://0.0.0.0:47291')

        # Changed. Default is false
        self.assertTrue(conf.SUPER_DEBUG, True)

        # Default True
        self.assertTrue(conf.HIDE_HEARTBEAT_LOGS)

        # Default is 4
        self.assertEqual(conf.CONCURRENT_JOBS, 4)

        # Default is (10, 'default')
        self.assertEqual(conf.QUEUES, [(10, conf.DEFAULT_QUEUE_NAME), ])

    def test_read_section(self):
        with utils.mock_config_file(self.settings_ini):
            settings.import_settings('jobmanager')

        # Changed
        self.assertFalse(conf.SUPER_DEBUG)
        # Changed
        self.assertEqual(conf.CONCURRENT_JOBS, 9283)

        # Changed
        self.assertEqual(conf.QUEUES,
                         [(50, 'google'), (40, 'pushes'), (10, 'default')])

        self.assertEqual(conf.WORKER_ADDR, 'tcp://160.254.23.88:47290')

    def test_invalid_section(self):
        conf.CONCURRENT_JOBS = 1234
        conf.QUEUES = [(50, 'google'), (40, 'pushes'), (10, 'default')]
        conf.WORKER_ADDR = 'tcp://160.254.23.88:47290'

        # Invalid section
        with utils.mock_config_file(self.settings_ini):
            settings.import_settings('nonexistent_section')

        # Overwritten values
        self.assertEqual(conf.CONCURRENT_JOBS, 1234)
        self.assertEqual(conf.QUEUES,
                         [(50, 'google'), (40, 'pushes'), (10, 'default')])
        self.assertEqual(conf.WORKER_ADDR, 'tcp://160.254.23.88:47290')
        # Default value
        self.assertEqual(conf.DEFAULT_QUEUE_NAME, 'default')

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
        reload(conf)

        with utils.mock_config_file(settings_ini):
            self.assertRaises(ValueError,
                              settings.import_settings, 'jobmanager')

    def test_parse_array(self):
        # Tests parsing a non-nested array (nested are tested via QUEUES
        # setting) via FAKE_VALUE
        settings_ini = "\n".join(
            ("[jobmanager]",
             'fake_value=["asdf", "asdf2"]',))
        # Ensure fresh test data
        reload(conf)
        conf.FAKE_VALUE = [u'pew', u'pew']

        with utils.mock_config_file(settings_ini):
            settings.import_settings('jobmanager')

        self.assertEqual(conf.FAKE_VALUE, [u'asdf', u'asdf2'])

    def test_invalid_setting(self):
        settings_ini = "\n".join(
            ("[global]",
             'nonexistent_setting=rabbit blood',))
        # Ensure fresh test data
        reload(conf)

        with LogCapture() as log_checker:
            with utils.mock_config_file(settings_ini):
                settings.import_settings()

            log_checker.check(
                ('eventmq.utils.settings',
                 'WARNING',
                 'Tried to set invalid setting: nonexistent_setting'))
