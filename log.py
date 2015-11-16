"""
log module for eventmq
"""
import logging

import zmq
import zmq.log.handlers


FORMAT_STANDARD = logging.Formatter(
    '%(asctime)s - %(name)s  %(levelname)s - "%(message)s')


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


def get_logger(name, formatter=FORMAT_STANDARD,
               handler=handlers.PUBLISH_HANDLER):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if handler == handlers.PUBLISH_HANDLER:
        _handler_sock = zmq.Context.instance().socket(zmq.PUB)
        _handler_sock.bind('tcp://127.0.0.1:33445')

        import time
        time.sleep(1)

        handler = handler(_handler_sock)
        handler.root_topic = name
    else:
        handler = handler()

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
