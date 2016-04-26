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
:mod:`devices` -- Device Utilities
==================================
"""


def generate_device_name(prefix=None):
    """
    This takes care of the python3 'everything is unicode' feature which
    causes errors when setting the IDENTITY of the ZMQ socket.

    Args:
        prefix (str): Prefix the id with this string.

    Returns (str) An ascii encoded string that can be used as an IDENTITY for a
        ZMQ socket.
    """
    import uuid
    ret = str(uuid.uuid4()).encode('ascii')
    if prefix:
        ret = prefix + ret
    return ret
