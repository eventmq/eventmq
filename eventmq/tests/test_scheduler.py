# -*- coding: utf-8 -*-
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
import json
import unittest

from .. import constants, scheduler

ADDR = 'inproc://pour_the_rice_in_the_thing'


class TestCase(unittest.TestCase):
    def test__setup(self):
        sched = scheduler.Scheduler(name='RuckusBringer')
        self.assertEqual(sched.name, 'RuckusBringer')

        self.assertFalse(sched.awaiting_startup_ack)
        self.assertEqual(sched.status, constants.STATUS.ready)

    def test_schedule_hash(self):
        msg1 = [
            'default',
            '',
            '3',
            json.dumps(['run', {
                'path': 'test',
                'args': [33, 'asdf'],
                'kwargs': {'zeta': 'Z', 'alpha': 'α'},
                'class_args': [0],
                'class_kwargs': {
                    'donkey': True, 'apple': False},
                'callable': 'do_the_thing'}]),
            None
        ]
        h1 = scheduler.Scheduler.schedule_hash(msg1)
        self.assertEqual('4658982cab9d32bf1ef9113a9d8bdec01775e2bc', h1)

        # Reordering the message argument shouldn't change the hash value
        msg2 = [
            'default',
            '',
            '3',
            json.dumps(['run', {
                'class_kwargs': {
                    'apple': False, 'donkey': True},
                'args': [33, 'asdf'],
                'class_args': [0],
                'kwargs': {'alpha': 'α', 'zeta': 'Z'},
                'path': 'test',
                'callable': 'do_the_thing'}]),
            None
        ]
        h2 = scheduler.Scheduler.schedule_hash(msg2)
        self.assertEqual('4658982cab9d32bf1ef9113a9d8bdec01775e2bc', h2)

# EMQP Tests
    def test_reset(self):
        sched = scheduler.Scheduler()

        self.assertFalse(sched.awaiting_startup_ack)
        self.assertEqual(sched.status, constants.STATUS.ready)
