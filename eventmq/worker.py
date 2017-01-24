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
"""
:mod:`worker` -- Worker Classes
===============================
Defines different short-lived workers that execute jobs
"""
from importlib import import_module
import logging
from multiprocessing import Process
import os
from threading import Thread

from . import conf

logger = logging.getLogger(__name__)


class MultiprocessWorker(Process):
    """
    Defines a worker that spans the job in a multiprocessing task
    """

    def __init__(self, input_queue, output_queue):
        super(MultiprocessWorker, self).__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.job_count = 0

    def run(self):
        """
        process a run message and execute a job

        This is designed to run in a seperate process.
        """
        import zmq
        zmq.Context.instance().term()

        # Pull the payload off the queue and run it
        for payload in iter(self.input_queue.get, 'DONE'):

            self.job_count += 1
            timeout = payload.get("timeout", None)
            msgid = payload.get('msgid', '')

            resp = {'msgid': msgid,
                    'return': 'None'}

            try:
                if timeout:
                    worker_thread = Thread(target=_run,
                                           args=(payload['params'], ))
                    worker_thread.start()
                    worker_thread.join(timeout)

                    if worker_thread.isAlive():
                        resp['return'] = 'TimeoutError'
                    else:
                        resp['return'] = 'DONE'
                else:
                    resp['reutrn'] = _run(payload['params'])
            except Exception as e:
                resp['return'] = str(e)

            resp['callback'] = payload['callback']
            self.output_queue.put(resp)

            if self.job_count > conf.MAX_JOB_COUNT:
                break

        logger.debug("Worker death, PID: {}".format(os.getpid()))


def _run(payload):
    if ":" in payload["path"]:
        _pkgsplit = payload["path"].split(':')
        s_package = _pkgsplit[0]
        s_cls = _pkgsplit[1]
    else:
        s_package = payload["path"]
        s_cls = None

    s_callable = payload["callable"]

    package = import_module(s_package)
    if s_cls:
        cls = getattr(package, s_cls)

        if "class_args" in payload:
            class_args = payload["class_args"]
        else:
            class_args = ()

        if "class_kwargs" in payload:
            class_kwargs = payload["class_kwargs"]
        else:
            class_kwargs = {}

        obj = cls(*class_args, **class_kwargs)
        callable_ = getattr(obj, s_callable)
    else:
        callable_ = getattr(package, s_callable)

    if "args" in payload:
        args = payload["args"]
    else:
        args = ()

    if "kwargs" in payload:
        kwargs = payload["kwargs"]
    else:
        kwargs = {}

    try:
        callable_(*args, **kwargs)
    except Exception as e:
        logger.exception(e)
        return str(e)
    # Signal that we're done with this job
    return 'DONE'
