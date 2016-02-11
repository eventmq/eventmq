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
import os.path  # used for deferjob test
from testfixtures import LogCapture
import unittest
import signal

from ..receiver import Receiver
from ..sender import Sender
from ..client import messages


class TestClass(object):
    """
    class to use in the build_module_path test
    """
    def mymethod(self):
        """
        this method is used as the callable in
        :meth:`TestCase.test_build_module_path`
        """
        return True


class CallableTestClass(object):
    def __call__(self):
        return True


class TestCase(unittest.TestCase):
    def test_defer_job(self):
        import json

        out = Sender()
        in_ = Receiver()

        in_.listen('ipc://test_defer_job')
        out.connect('ipc://test_defer_job')

        messages.defer_job(out, os.path.walk, reply_requested=True,
                           guarantee=True, retry_count=3, queue='test_queue')

        # An index error here means the frames weren't properly formatted
        msg = in_.recv_multipart()[7]

        self.assertEqual(json.loads(msg), ["run", {"args": [],
                                                   "class_args": [],
                                                   "callable": "walk",
                                                   "kwargs": {},
                                                   "path": "posixpath",
                                                   "class_kwargs": {}}])

        with LogCapture() as log_checker:
            # don't blow up on a non-callable
            messages.defer_job(out, 'non-callable')

            # don't blow up on some random nameless func
            def nameless_func():
                return True
            nameless_func.func_name = ''
            messages.defer_job(out, nameless_func)

            # don't blow-up for pathless functions
            nameless_func.func_name = 'nameless_func'
            nameless_func.__module__ = None
            messages.defer_job(out, nameless_func)

            # log an error if a callable instance object is passed
            callable_obj = CallableTestClass()
            messages.defer_job(out, callable_obj)

            log_checker.check(
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered non-callable func: non-callable'),
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered callable with no name in '
                 'eventmq.tests.test_client_messages'),
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered callable with no __module__ path nameless_func'),
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered unknown callable ({}) type instanceobject'.
                 format(callable_obj)),
                ('eventmq.client.messages',
                 'ERROR',
                 'Encountered callable with no name in '
                 'eventmq.tests.test_client_messages'),
            )



    def test_build_module_path(self):
        funcpath = messages.build_module_path(os.path.walk)

        t = TestClass()
        methpath = messages.build_module_path(t.mymethod)

        self.assertEqual(funcpath, ('posixpath', 'walk'))
        self.assertEqual(methpath,
            ('eventmq.tests.test_client_messages:TestClass', 'mymethod'))
