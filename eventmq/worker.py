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
from threading import Thread

from . import conf
from .utils.functions import callable_from_name

if sys.version[0] == '2':
    import Queue
else:
    import queue as Queue


class MultiprocessWorker(Process):
    """
    Defines a worker that spans the job in a multiprocessing task
    """

    def __init__(self, input_queue, output_queue, ppid, run_setup=True):
        super(MultiprocessWorker, self).__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.job_count = 0
        self.run_setup = run_setup
        self.ppid = ppid

    @property
    def logger(self):
        return logging.getLogger(__name__ + '.' + str(os.getpid()))

    def run(self):
        """
        process a run message and execute a job

        This is designed to run in a seperate process.
        """
        # Define the 2 queues for communicating with the worker thread
        logger = self.logger

        worker_queue = Queue.Queue(1)
        worker_result_queue = Queue.Queue(1)
        worker_thread = Thread(target=_run,
                               args=(worker_queue,
                                     worker_result_queue,
                                     logger))

        import zmq
        zmq.Context.instance().term()

        self.output_queue.put(
            {'msgid': None,
             'return': None,
             'death': False,
             'pid': os.getpid(),
             'callback': 'worker_ready'}
        )

        callback = 'premature_death'

        worker_thread.start()

        # Main execution loop, only break in cases that we can't recover from
        # or we reach job count limit
        while True:
            try:
                payload = self.input_queue.get(timeout=1)
                if payload == 'DONE':
                    break

            except Queue.Empty:
                if os.getppid() != self.ppid:
                    break
                continue
            except Exception:
                break
            finally:
                # If I'm an orphan, die
                if os.getppid() != self.ppid:
                    break

            try:
                return_val = 'None'
                self.job_count += 1
                timeout = payload.get("timeout") or conf.GLOBAL_TIMEOUT
                msgid = payload.get('msgid', '')
                callback = payload.get('callback', '')

                if conf.SUPER_DEBUG:
                    logger.debug("Putting on thread queue msgid: {}".format(
                        msgid))

                worker_queue.put(payload['params'])

                try:
                    return_val = worker_result_queue.get(timeout=timeout)

                    if conf.SUPER_DEBUG:
                        logger.debug("Got from result queue msgid: {}".format(
                            msgid))
                except Queue.Empty:
                    return_val = 'TimeoutError'

                return_val = {"value": return_val}

                try:
                    self.output_queue.put_nowait(
                        {'msgid': msgid,
                         'return': return_val,
                         'death': self.job_count >= conf.MAX_JOB_COUNT or
                         return_val["value"] == 'TimeoutError',
                         'pid': os.getpid(),
                         'callback': callback}
                    )
                except Exception:
                    break

                if return_val["value"] == 'TimeoutError':
                    break

            except Exception as e:
                return_val = str(e)

            if self.job_count >= conf.MAX_JOB_COUNT:
                logger.debug("Worker reached job limit, exiting")
                break

        worker_queue.put('DONE')
        worker_thread.join(timeout=5)

        self.output_queue.put(
            {'msgid': None,
             'return': 'DEATH',
             'death': True,
             'pid': os.getpid(),
             'callback': 'worker_death'}
            )

        logger.debug("Worker death")

        if worker_thread.is_alive():
            logger.debug("Worker thread did not die gracefully")


def _run(queue, result_queue, logger):
    """
    Takes care of actually executing the code given a message payload

    Example payload:
    {
        "path": "path_to_callable",
        "callable": "name_of_callable",
        "args": (1, 2),
        "kwargs": {"value": 1},
        "class_args": (3, 4),
        "class_kwargs": {"value": 2}
    }
    """
    if any(conf.SUBPROCESS_SETUP_FUNC):
        try:
            logger.debug("Running setup ({}) for worker id {}".format(
                conf.SUBPROCESS_SETUP_FUNC, os.getpid()))
            setup_func = callable_from_name(conf.SUBPROCESS_SETUP_FUNC)
            setup_func()
        except Exception as e:
            logger.warning('Unable to do setup task ({}): {}'
                           .format(conf.SUBPROCESS_SETUP_FUNC, str(e)))

    elif any(conf.SETUP_CALLABLE) and any(conf.SETUP_PATH):
        logger.warning("SETUP_CALLABLE and SETUP_PATH deprecated in favor for "
                       "SUBPROCESS_SETUP_FUNC")
        try:
            logger.debug("Running setup ({}.{}) for worker id {}"
                         .format(
                             conf.SETUP_PATH,
                             conf.SETUP_CALLABLE,
                             os.getpid()))
            run_setup(conf.SETUP_PATH, conf.SETUP_CALLABLE)
        except Exception as e:
            logger.warning('Unable to do setup task ({}.{}): {}'
                           .format(conf.SETUP_PATH,
                                   conf.SETUP_CALLABLE, str(e)))

    if conf.JOB_ENTRY_FUNC:
        job_entry_func = callable_from_name(conf.JOB_ENTRY_FUNC)
    else:
        job_entry_func = None

    if conf.JOB_EXIT_FUNC:
        job_exit_func = callable_from_name(conf.JOB_EXIT_FUNC)
    else:
        job_exit_func = None

    while True:
        # Blocking get so we don't spin cycles reading over and over
        try:
            payload = queue.get()
        except Exception as e:
            logger.exception(e)
            continue

        if payload == 'DONE':
            break

        if job_entry_func:
            job_entry_func()

        return_val = _run_job(payload, logger)

        if job_exit_func:
            job_exit_func()

        # Signal that we're done with this job and put its return value on the
        # result queue
        result_queue.put(return_val)

    logger.debug("Worker thread death")


def _run_job(payload, logger):
    try:
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

        return_val = callable_(*args, **kwargs)
    except Exception as e:
        logger.exception(e)
        return_val = str(e)

    return return_val


def run_setup(setup_path, setup_callable):
    """
    Runs the initial setup code of a given worker process by executing the code
    at setup_path.setup_callable().  Note only functions are supported, no
    class methods
    """
    if ":" in setup_path:
        _pkgsplit = setup_path.split(':')
        s_setup_package = _pkgsplit[0]
    else:
        s_setup_package = setup_path

    if setup_callable and s_setup_package:
        setup_package = import_module(s_setup_package)

        setup_callable_ = getattr(setup_package, setup_callable)

        return setup_callable_()
