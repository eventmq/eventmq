"""
Simulates app code to publish messages to the workers
"""
import logging
import time
import zmq

logger = logging.getLogger('publisher')
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)


# You should create and use exactly one context in your process.
context = zmq.Context()


if __name__ == "__main__":
    logger.debug('Starting...')
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect('tcp://127.0.0.1:6988')

    ip_socket = context.socket(zmq.REP)
    ip_socket.bind('inproc://vb934')
    msgn = context.socket(zmq.REQ)
    msgn.connect('inproc://vb934')

    ext_socket = context.socket(zmq.REP)
    ext_socket.bind('tcp://127.0.0.1:9999')

    poller = zmq.Poller()
    poller.register(ip_socket, zmq.POLLIN)
    poller.register(ext_socket, zmq.POLLIN)
    poller.register(msgn, zmq.POLLIN)

    while True:
        events = dict(poller.poll())

        if ext_socket in events and events[ext_socket] == zmq.POLLIN:
            ext_msg = ext_socket.recv_multipart()
            logger.debug('ext: %s' % ext_msg)
            msgn.send_multipart(ext_msg)

        if ip_socket in events and events[ip_socket] == zmq.POLLIN:
            ip_msg = ip_socket.recv_multipart()
            logger.debug('ip_msg: %s' % ip_msg)
            pub_socket.send_multipart(ip_msg)
            ip_socket.send_multipart(['asdf', 'Scheduled.'])

        if msgn in events and events[msgn] == zmq.POLLIN:
            ip_reply = msgn.recv_multipart()
            logger.debug('extreply: %s' % ip_reply)
            ext_socket.send_multipart(ip_reply)
