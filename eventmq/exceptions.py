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
:mod:`exceptions` -- Exceptions
===============================
These are the exceptions that are raised by EventMQ. All exceptions should
be subclasses of :class:`EventMQError`
"""


class EventMQError(Exception):
    """
    All exceptions raised by EventMQ inherit from this base exception.
    """


class MessageError(EventMQError):
    """
    Raised when there is a problem with the structure of the message.
    """


class InvalidMessageError(MessageError):
    """
    Raise when EventMQ encounters a malformed message is encountered.
    """


class PeerGoneAwayError(EventMQError):
    """
    Raised when attempting to contact a peer that no longer exists (i.e. when
    sending a message to it)
    """


class NoAvailableWorkerSlotsError(EventMQError):
    """
    Raised when there is no available workers for a job manager.
    """


class UnknownQueueError(EventMQError):
    """
    Raised when a queue is not found in the internal list of queues.
    """


class CallableFromPathError(EventMQError):
    """
    Raised when construction of a callable from a path and callable_name fails.
    """
    pass


class ConnectionError(EventMQError):
    """
    Raised when there is an error connecting to a network service.
    """
