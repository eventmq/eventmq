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
import sys

from threading import Event, Thread

from . import conf


if sys.version[0] == '2':
    import Queue
else:
    import queue as Queue


class StoppableThread(Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, target, name=None, args=()):
        super(StoppableThread, self).__init__(name=name, target=target,
                                              args=args)
        self._return = None
        self._stop = Event()

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)

    def join(self, timeout=None):
        Thread.join(self, timeout=timeout)
        return {'value': self._return}


class MultiprocessWorker(Process):
    """
    Defines a worker that spans the job in a multiprocessing task
    """

    def __init__(self, input_queue, output_queue, run_setup=True):
        super(MultiprocessWorker, self).__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.job_count = 0
        self.run_setup = run_setup
        self.ppid = os.getppid()

    @property
    def logger(self):
        if not hasattr(self, '_logger'):
            self._logger = logging.getLogger(__name__ + '.' + str(os.getpid()))

        return self._logger

    def run(self):
        """
        process a run message and execute a job

        This is designed to run in a seperate process.
        """
        if self.run_setup:
            self.run_setup = False
            if any(conf.SETUP_CALLABLE) and any(conf.SETUP_PATH):
                try:
                    self.logger.debug("Running setup ({}.{}) for worker id {}"
                                      .format(
                                          conf.SETUP_PATH,
                                          conf.SETUP_CALLABLE,
                                          os.getpid()))
                    run_setup(conf.SETUP_PATH, conf.SETUP_CALLABLE,
                              self.logger)
                except Exception as e:
                    self.logger.warning('Unable to do setup task ({}.{}): {}'
                                        .format(conf.SETUP_PATH,
                                                conf.SETUP_CALLABLE, str(e)))

        import zmq
        zmq.Context.instance().term()

        death_callback = 'worker_death'

        while True:
            try:
                payload = self.input_queue.get_nowait()
                if payload == 'DONE':
                    break
            except Queue.Empty:
                continue
            except Exception as e:
                    break
            finally:
                if os.getppid() == 1:
                    break

            try:
                return_val = 'None'
                self.logger.debug("Job started")
                self.job_count += 1
                timeout = payload.get("timeout", None)
                msgid = payload.get('msgid', '')

                if timeout:
                    worker_thread = StoppableThread(target=_run,
                                                    args=(payload['params'],
                                                          self.logger))
                    worker_thread.start()
                    return_val = worker_thread.join(timeout)

                    if worker_thread.isAlive():
                        worker_thread.stop()
                        self.output_queue.put({
                            {'msgid': msgid,
                             'return': 'TimeoutError',
                             'pid': os.getpid(),
                             'callback': payload['callback']}
                        })
                        break

                else:
                    return_val = _run(payload['params'])

            except Exception as e:
                return_val = str(e)

            if self.job_count >= conf.MAX_JOB_COUNT \
               or return_val == 'TimeoutError':
                death_callback = 'worker_death_with_reply' \
                                 if 'reply' in payload['callback'] else \
                                    'worker_death'
                break

            else:
                self.output_queue.put(
                    {'msgid': msgid,
                     'return': return_val,
                     'pid': os.getpid(),
                     'callback': payload['callback']}
                )

        self.output_queue.put(
            {'msgid': None,
             'return': 'DEATH',
             'pid': os.getpid(),
             'callback': death_callback}
            )
        self.logger.debug("Worker death")


def _run(payload, logger):
    """
    Takes care of actually executing the code given a message payload
    """
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
        return_val = callable_(*args, **kwargs)
    except Exception as e:
        logger.exception(e)
        return str(e)

    # Signal that we're done with this job
    return return_val


def run_setup(setup_path, setup_callable):
    if ":" in setup_path:
        _pkgsplit = setup_path.split(':')
        s_setup_package = _pkgsplit[0]
    else:
        s_setup_package = setup_path

    if setup_callable and s_setup_package:
        setup_package = import_module(s_setup_package)

        setup_callable_ = getattr(setup_package, setup_callable)

        setup_callable_()
