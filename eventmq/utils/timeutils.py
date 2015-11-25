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
:mod:`timeutils` -- Time Utilites
=================================
"""
try:
    from time import monotonic  as _monotonic  # Python3
except ImportError:
    from monotonic import monotonic as _monotonic
from time import time as _time


def timestamp():
    """
    """
    return _time()


def monotonic():
    """
    """
    return _monotonic()
