"""
Recieve some message and do some work
"""
import logging
import zmq

logger = logging.getLogger('worker')
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)

REQUEST_TIMEOUT = 2500  # ms
REQUEST_RETRIES = 3     # before giving up
SERVER_ENDPOINT = 'tcp://127.0.0.1:6989'


# You should create and use exactly one context in your process.
context = zmq.Context(1)


def get_new_sub_socket(subscription='', host_url=SERVER_ENDPOINT):
    """
    Create and return a new subscription socket for recieving messages

    Args:
        subscription (str): name of the subscription topic you wish to
            subscribe to. default is all message. Note: This is evaluated as a
            prefix, any subscriptions which begin with this value will be
            subscribed to.
        host_url (str): the server which is publishing messages for us to
            consume
    """
    r = context.socket(zmq.SUB)
    r.connect(host_url)
    r.setsockopt(zmq.SUBSCRIBE, '')

    return r


if __name__ == "__main__":
    msg_count = 0
    poller = zmq.Poller()
    receiver = get_new_sub_socket()
    poller.register(receiver, zmq.POLLIN)

    while True:
        try:
            socks = (dict(poller.poll()))
        except KeyboardInterrupt:
            logger.debug ('Processed %d messages.' % msg_count)
        if receiver in socks and socks[receiver] == zmq.POLLIN:
            message = receiver.recv_multipart()
            msg_count += 1
            logger.debug('Recieved message: %s' % message)
