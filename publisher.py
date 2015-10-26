"""
Simulates app code to publish messages to the workers
"""
import logging
import time
import zmq

logger = logging.getLogger('publisher')
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)


# You should create and use exactly one context in your process.
context = zmq.Context()


if __name__ == "__main__":
    logger.debug('Starting...')
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://*:9687')
    logger.debug('Listening on :9687')
    while True:
        logger.debug('True')
        socket.send(b'hello')
