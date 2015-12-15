# This file is part of eventmq.
#
# eventmq is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# eventmq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with eventmq.  If not, see <http://www.gnu.org/licenses/>.
"""
:mod:`worker` -- Worker Classes
===============================
Defines different short-lived workers that execute jobs
"""
from importlib import import_module
import logging
import multiprocessing

logger = logging.getLogger(__name__)


class MultiprocessWorker(object):
    """
    Defines a worker that spans the job in a multiprocessing task
    """
    def run(self, payload):
        """
        process a message
        """
        ### Spawn in a new multiprocess
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


        # try:
        #     callable_(*args, **kwargs)
        # except Exception as e:
        #     logger.exception(e.message)

        multiprocessing.Process(target=callable_, args=args, kwargs=kwargs).start()
