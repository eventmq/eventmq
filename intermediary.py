"""
Handle connecting pub/sub sockets
"""
import logging
import zmq

logger = logging.getLogger('publisher')
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)

context = zmq.Context()


if __name__ == "__main__":
    xsub = context.socket(zmq.XSUB)
    xsub.bind('tcp://*:6988')
    xpub = context.socket(zmq.XPUB)
    xpub.bind('tcp://*:6989')

    poller = zmq.Poller()
    poller.register(xpub, zmq.POLLIN)
    poller.register(xsub, zmq.POLLIN)

    while True:
        events = dict(poller.poll(1000))
        if xpub in events:
            message = xpub.recv_multipart()
            print "[BROKER] subscription message: %r" % message[0]
            xsub.send_multipart(message)
        if xsub in events:
            message = xsub.recv_multipart()
            print "publishing message: %r" % message
            xpub.send_multipart(message)
