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
:mod:`utils` -- Utilities
=========================
This module contains a handful of utility classes to make dealing with things
like creating message more simple.
"""
import uuid


def generate_msgid():
    """
    Returns a (universally) unique id to be used for messages
    """
    return str(uuid.uuid4())


def create_message(queue_name, message,
                   reply_requested=False, fail_quota=0, retry_count=0):
    return (queue_name, message)

if __name__ == "__main__":
    pass
