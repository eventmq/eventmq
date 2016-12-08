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
from .utils.functions import run_function

from multiprocessing import Pool, TimeoutError

# the run function is executed in a different process, so we need to set the
# logger up.
from . import log

logger = log.setup_logger(__name__)


def run(payload, msgid, timeout=None):
    """
    executes job in a thread, killing it after a specified timeout
    """
    if timeout:
        worker = Pool(1)
        result = worker.apply_async(_run, args=(payload, msgid))

        try:
            out = result.get(timeout)
            return out
        except TimeoutError:
            worker.terminate()
            return (msgid, 'TimeoutError')
    else:
        return _run(payload, msgid)


def _run(payload, msgid):
    """
    process a run message and execute a job

    This is designed to run in a seperate process.
    """
    # deconstruct the payload
    path = payload.get('path')
    callable_name = payload.get('callable')
    class_args = payload.get('class_args', tuple()) or tuple()
    class_kwargs = payload.get('class_kwargs', dict()) or dict()
    args = payload.get('args', tuple()) or tuple()
    kwargs = payload.get('kwargs', dict()) or dict()

    try:
        r = run_function(
            callable_name='{}.{}'.format(path, callable_name),
            class_args=class_args or (),
            class_kwargs=class_kwargs or {},
            args=args or (),
            kwargs=kwargs or {})
        return (msgid, r)
    except Exception as e:
        logger.exception(e)
        return (msgid, str(e))

    # Signal that we're done with this job
    return (msgid, '')
