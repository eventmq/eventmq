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
log module for eventmq

this needs so much work.
"""
import logging

import zmq
import zmq.log.handlers


FORMAT_STANDARD = logging.Formatter(
    '%(asctime)s - %(name)s  %(levelname)s - %(message)s')
FORMAT_NAMELESS = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')


class PUBHandler(zmq.log.handlers.PUBHandler):
    """
    """
    pass


class handlers(object):
    """
    log handlers

    PUBLISH_HANDLER - blast logs through a pub mechanism
    STREAM_LOGGER - logs to stdout/stderr
    """
    PUBLISH_HANDLER = PUBHandler
    STREAM_HANDLER = logging.StreamHandler


def setup_logger(base_name, formatter=FORMAT_STANDARD,
                 handler=handlers.STREAM_HANDLER):

    logger = logging.getLogger(base_name)
    logger.setLevel(logging.DEBUG)

    # remove handlers we don't want
    #for h in logger.handlers:
    #    logger.removeHandler(h)

    if handler == handlers.PUBLISH_HANDLER:
        _handler_sock = zmq.Context.instance().socket(zmq.PUB)
        _handler_sock.bind('tcp://127.0.0.1:33445')

        import time
        time.sleep(1)

        handler = handler(_handler_sock)
        handler.root_topic = base_name
    else:
        handler = handler()

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
