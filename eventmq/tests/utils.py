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
from configparser import ConfigParser
from contextlib import contextmanager
import io
import sys
import uuid

import mock
import zmq

from .. import conf, constants
from ..utils.classes import ZMQReceiveMixin, ZMQSendMixin
from ..utils.devices import generate_device_name


class FakeDevice(ZMQReceiveMixin, ZMQSendMixin):
    """
    A fake router device so we can test with some of the nice utilities, but
    still allowing manual control
    """
    def __init__(self, type_=zmq.ROUTER):
        super(FakeDevice, self).__init__()

        self.name = generate_device_name()
        self.zsocket = zmq.Context.instance().socket(type_)
        self.zsocket.setsockopt(zmq.IDENTITY, self.name)


def send_raw_INFORM(sock, type_, queues=(conf.DEFAULT_QUEUE_NAME,)):
    """
    Send an INFORM message to a raw ZMQ socket.

    Args:
        sock: Socket to send the message on
        type_: Type of client. One of ``scheduler`` or ``worker``
        queues (list, tuple): List of queues to listen on

    Return:
        str: The message id of the INFORM message
    """
    msgid = str(uuid.uuid4())
    tracker = sock.zsocket.send_multipart((
        '',
        constants.PROTOCOL_VERSION,
        'INFORM',
        msgid,
        ','.join(queues),
        type_
    ), copy=False, track=True)
    tracker.wait(1)

    return msgid


def send_raw_READY(sock):
    """
    Sends a READY message to a raw ZMQ socket.

    Args:
        sock: Socket to send the message on

    Return:

    """
    msgid = str(uuid.uuid4())
    tracker = sock.zsocket.send_multipart((
        '',
        constants.PROTOCOL_VERSION,
        'READY',
        msgid
    ), copy=False, track=True)
    tracker.wait(1)

    return msgid


@contextmanager
def mock_config_file(settings_ini):
    """
    Mocks reading eventmq configuration file from the provided ``config``
    string.

    .. code:: python

       from eventmq.utils.settings import import_settings
       from eventmq.tests.utils import mock_config_file

       settings_ini = "\n".join(
           ("[global]",
            "super_debug=TRuE",
            "frontend_addr=tcp://0.0.0.0:47291",
            "",
            "[jobmanager]",
            "super_debug=FalSe",
            'queues=[[50,"google"], [40,"pushes"], [10,"default"]]',
            "worker_addr=tcp://160.254.23.88:47290",
            "concurrent_jobs=9283",))

       with mock_config_file(settings_ini):
           import_settings('jobmanager')

    Args:
        settings_ini (str): INI-style config provided as a string
    """
    _config = ConfigParser()

    if sys.version_info[0] == 3:
        _config.read_string(settings_ini)
    else:
        _config.readfp(io.BytesIO(settings_ini))

    with\
        mock.patch('eventmq.utils.settings.ConfigParser',
                    return_value=_config), \
        mock.patch('eventmq.utils.settings.os.path.exists',
                   return_value=True), \
        mock.patch.object(_config, 'read'):  # noqa
        yield
