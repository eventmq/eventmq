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
from past.builtins import basestring
from six.moves import range

from .. import conf


def encodify(message):
    """
    Recursivly ensure all strings have been encoded to
    ``conf.DEFAULT_ENCODING``.

    Args:
        message: any primitive object
    """
    if isinstance(message, (tuple, list)):
        # Tuples are typecast to lists so self-assignment works and then
        # converted back to a tuple.
        is_tuple = False
        if isinstance(message, tuple):
            message = list(message)
            is_tuple = True

        for i in range(0, len(message)):
            message[i] = encodify(message[i])

        if is_tuple:
            message = tuple(message)
    elif isinstance(message, dict):
        for k in message:
            message[k] = encodify(message[k])
    elif isinstance(message, basestring):
        return message.encode(conf.DEFAULT_ENCODING)
    else:
        return message
    return message
