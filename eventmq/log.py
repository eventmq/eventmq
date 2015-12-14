"""
log module for eventmq

this needs so much work.
"""
import logging

import zmq
import zmq.log.handlers

import watchtower


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
    CLOUDWATCH_HANDLER = watchtower.CloudWatchLogHandler


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
    elif handler == handlers.CLOUDWATCH_HANDLER:
        handler = handler(log_group='eventmq-dev')
    else:
        handler = handler()

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
