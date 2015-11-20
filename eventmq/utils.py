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

from . import exceptions


def generate_msgid():
    """
    Returns a (universally) unique id to be used for messages
    """
    return str(uuid.uuid4())


def parse_message(message):
    """
    Parses the generic format of an eMQP/1.0 message and returns the
    parts.

    Args:
        message: the message you wish to have parsed

    Returns (tuple) (sender_id, command, message_id, (message_body, and_data))
    """
    try:
        sender = message[0]
        # noop = message[1]
        # protocol_version = message[2]
        command = message[3]
        msgid = message[4]
    except IndexError:
        raise exceptions.InvalidMessageError('Invalid Message Encountered: %s'
                                             % str(message))
    if len(message) > 5:
        msg = message[5:]
    else:
        msg = ()
    return (sender, command, msgid, msg)
