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
from multiprocessing import Pool
import time

from nose import with_setup

from .. import worker

ADDR = 'inproc://pour_the_rice_in_the_thing'


def setup_func():
    global pool
    global out
    pool = Pool()
    out = pool.map(job, range(1))


@with_setup(setup_func)
def test_run_with_timeout():
    payload = {
        'path': 'eventmq.tests.test_worker',
        'callable': 'job',
        'args': [2]
    }

    msgid = worker._run_job(payload, logging.getLogger())

    assert msgid


@with_setup(setup_func)
def test_run_setup():
    setup_callable = 'pre_hook'
    setup_path = 'eventmq.tests.test_worker'

    worker.run_setup(setup_path, setup_callable)


def job(sleep_time=0):
    time.sleep(sleep_time)

    return True


def pre_hook():
    return 1


def post_hook():
    return 1
