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

from .. import constants, scheduler

ADDR = 'inproc://pour_the_rice_in_the_thing'


class TestCase(unittest.TestCase):
    def test__setup(self):
        sched = scheduler.Scheduler(name='RuckusBringer')
        self.assertEqual(sched.name, 'RuckusBringer')

        self.assertFalse(sched.awaiting_startup_ack)
        self.assertEqual(sched.status, constants.STATUS.ready)

# EMQP Tests
    def test_reset(self):
        sched = scheduler.Scheduler()

        self.assertFalse(sched.awaiting_startup_ack)
        self.assertEqual(sched.status, constants.STATUS.ready)
