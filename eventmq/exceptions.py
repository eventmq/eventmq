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
:mod:`exceptions` -- Exceptions
===============================
These are the exceptions that are raised by EventMQ. All exceptions should
be subclasses of :class:`EventMQError`
"""


class EventMQError(Exception):
    """
    All exceptions raised by EventMQ inherit from this base exception
    """


class MessageError(EventMQError):
    """
    Raised when there is a problem with the structure of the message
    """


class InvalidMessageError(MessageError):
    """
    Raise when EventMQ encounters a malformed message is encountered.
    """
