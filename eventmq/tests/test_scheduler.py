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

import mock

from .. import constants, scheduler, utils


class TestCase(unittest.TestCase):
    @mock.patch('uuid.uuid4')
    def test__setup(self, name_mock):
        name_mock.return_value = 'some_uuid'
        override_settings = {
            'NAME': 'RuckasBringer'
        }
        sched = scheduler.Scheduler(override_settings=override_settings)
        self.assertEqual(sched.name.decode('ascii'), 'RuckasBringer:some_uuid')

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

    @mock.patch.object(utils.classes.ZMQSendMixin, 'send_multipart')
    def test_on_schedule(self, send_mock):
        override_settings = {}
        sched = scheduler.Scheduler(override_settings=override_settings)

        job_msg = json.dumps(['run', {
            'path': 'test',
            'args': [33, 'asdf'],
            'kwargs': {'zeta': 'Z', 'alpha': 'α'},
            'class_args': [0],
            'class_kwargs': {
                'donkey': True, 'apple': False},
            'callable': 'do_the_thing'}])

        msg = [
            'default',
            'run_count:3,guarantee',
            '3',
            job_msg,
            None
        ]

        cron_msg = [
            'default',
            'run_count:3,guarantee',
            '-1',
            job_msg,
            '* * * * *',
        ]

        sched.on_schedule('fake_msgid', msg)
        self.assertEqual(1, len(sched.interval_jobs))
        self.assertEqual(0, len(sched.cron_jobs))

        self.assertEqual(1, len(json.loads(
            sched.get_scheduled_jobs())['interval_jobs']))
        self.assertEqual(0,
                         len(json.loads(
                             sched.get_scheduled_jobs())['cron_jobs']))

        # Scheduling the same job as a cron should remove it from interval
        sched.on_schedule('fake_msgid2', cron_msg)
        self.assertEqual(0, len(sched.interval_jobs))
        self.assertEqual(1, len(sched.cron_jobs))

        self.assertEqual(0, len(json.loads(
            sched.get_scheduled_jobs())['interval_jobs']))
        self.assertEqual(1, len(json.loads(
            sched.get_scheduled_jobs())['cron_jobs']))

        # Change the job message and it should make new jobs
        job_msg = json.dumps(['run', {
            'path': 'test',
            'args': [333, 'asdf'],
            'kwargs': {'zeta': 'Z', 'alpha': 'α'},
            'class_args': [0],
            'class_kwargs': {
                'donkey': True, 'apple': False},
            'callable': 'do_the_thing'}])

        msg[3] = job_msg

        sched.on_schedule('fake_msgid3', msg)
        self.assertEqual(1, len(sched.interval_jobs))
        self.assertEqual(1, len(sched.cron_jobs))

        self.assertEqual(1, len(json.loads(
            sched.get_scheduled_jobs())['interval_jobs']))
        self.assertEqual(1, len(json.loads(
            sched.get_scheduled_jobs())['cron_jobs']))

        # Make sure the scheduler obeyed 3 schedule commands with 'haste'
        self.assertEqual(3, send_mock.call_count)


# EMQP Tests
    def test_reset(self):
        sched = scheduler.Scheduler(
            override_settings={
                "ADMINISTRATIVE_LISTEN_ADDR": "ipc://forkthis:12345"
            })

        self.assertFalse(sched.awaiting_startup_ack)
        self.assertEqual(sched.status, constants.STATUS.ready)
