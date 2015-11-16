"""
eventmq router

routes messages to workers in  named queues
"""
import uuid

import zmq
from zmq.eventloop import ioloop, zmqstream

from eventmq import STATUS
import log

logger = log.get_logger(__file__)


class Router(object):
    """
    A simple router of messages
    """

    def __init__(self, *args, **kwargs):
        self.name = uuid.uuid4()
        self.logger = logger
        self.socket_ctx = kwargs.get('context') or zmq.Context.instance()

        self.incoming_sock = zmqstream.ZMQStream(self.socket_ctx.socket(
            zmq.ROUTER))
        self.outgoing = zmqstream.ZMQStream(self.socket_ctx.socket(zmq.DEALER))

        self.incoming.on_recv(self.on_receive_request)
        self.status = STATUS.ready
        self.logger.info('Initialized Router...')

    def listen(self,
               frontend_addr='tcp://127.0.0.1:47290',
               backend_addr='tcp://127.0.0.1:47291'):
        """
        Being listening for connections on the provided connection strings

        :param frontend_addr: connection string to listen for requests
        :type incoming: str
        :param backend_addr: connection string to listen for workers
        :type outgoing: str
        """
        self.status = STATUS.starting

        self.incoming.bind(frontend_addr)
        self.outgoing.bind(backend_addr)

        self.status = STATUS.started
        self.logger.info('Listening for requests on %s' % frontend_addr)
        self.logger.info('Listening for workers on %s' % backend_addr)

    def on_receive_request(self, msg):
        print('recvd')
        self.logger.debug(msg)


if __name__ == "__main__":
    ioloop.install()
    r = Router()
    r.listen()

    import eventmq
    eventmq.send_msg('test')
    ioloop.IOLoop.instance().start()
