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
:mod:`timeutils` -- Time Utilites
=================================
"""
try:
    from time import monotonic as _monotonic  # Python3
except ImportError:
    from monotonic import monotonic as _monotonic
from time import time as _time


def timestamp():
    """
    """
    return _time()


def monotonic():
    """
    Returns (float) seconds since boot, or something close to it. This value
    will never count down so it's useful for cases where DST would mess up
    time.time() arithmetic (e.g. heartbeats).
    """
    return _monotonic()


def seconds_until(ts):
    """
    Calculates the number of seconds until `ts` by subtracting it from
    time.time()
    """
    return ts - timestamp()


class IntervalIter(object):
    """
    represents an interval (in seconds) and it's `next()` execution time

    Usage:
        # interval of 5min using monotonic clock (assume it starts at 0 for the
        # sake of the example)
        interval = IntervalIter(monotonic, 300)
        # Py2

        interval.next()  # 300
        interval.next()  # 600

        # Py3
        next(interval)  # 300
        next(interval)  # 600
    """
    def __init__(self, start_value, interval_secs):
        """
        Args:
            start_value (numeric): the timestamp to begin with. usually gotten
                via :func:`monotonic` or :func:`timestamp`
            interval_secs (int): the number of seconds between intervals
        """
        self.current = start_value
        self.interval_secs = interval_secs

        # iterate so the first call to .next() is `interval_secs` ahead of the
        # initial `start_value`
        self.__next__()

    def __iter__(self):
        return self

    def __next__(self):  # Py3
        self.current += self.interval_secs
        return self.current - self.interval_secs

    def next(self):
        return self.__next__()
