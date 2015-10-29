"""
eventmq
"""
import logging
import threading
import zmq
Context = zmq.Context

LOG_LEVEL = logging.DEBUG
PROTOCOLS = ('tcp', 'udp', 'pgm', 'epgm', 'inproc', 'ipc')
VALID_TOPIC_TYPES = (int, str)

MSG_RUREADY = lambda t: [t, 'RUREADY']
MSG_READ = lambda t: [t, 'READY']


class STATUS(object):
    ready = 0
    started = 2
    stopping = 3


class LoggerMixin(object):
    """
    Provides self.logger
    """
    FORMAT_STANDARD = logging.Formatter('%(asctime)s - %(name)s - '
                                        '%(levelname)s - %(message)s')

    def __init__(self, *args, **kwargs):
        super(LoggerMixin, self).__init__(*args, **kwargs)
        self._stream_handler = logging.StreamHandler()
        self._logger = None
        self._stream_handler.setFormatter(self.FORMAT_STANDARD)

    @property
    def logger(self):
        if not self._logger:
            self._logger = logging.getLogger(self.__class__.__name__.lower())
            self._logger.setLevel(LOG_LEVEL)
            self._logger.addHandler(self._stream_handler)
        return self._logger


class Message(object):
    class FLAGS:
        enquire = 'ready?'  # Are you ready?
        acknowledge = 'ready!'  # Ready

    def __init__(self, topic='', message=None):
        self.topic = topic
        self.message = message

    @property
    @classmethod
    def ENQUIRE(cls):
        return cls(message=cls.FLAGS.enquire)


class Publisher(LoggerMixin):
    """
    Handles sockets for publishing messages

    Attrs:
        context (:class:`zmq.Context`)
        socket_type
        socket (:class:`zmq.Socket`): raw zmq socket
    """

    def __init__(self, bidirectional=False, context=None):
        """
        """
        super(Publisher, self).__init__()
        self.context = context or zmq.Context.instance()
        self.socket_type = zmq.XPUB if bidirectional else zmq.PUB
        self.socket = self.context.socket(self.socket_type)
        self.status = STATUS.ready

    def listen(self, addr="tcp://127.0.0.1:47331"):
        """
        Starts listening on an address
        """
        self.socket.bind(addr)
        self.status = STATUS.started

    def connect(self, addr='tcp://127.0.0.1:47330'):
        self.socket.connect(addr)
        if self.socket_type == zmq.XPUB:
            self.socket.send_multipart(['', Message.FLAGS.enquire])

    def close(self):
        """
        Close the socket
        """
        self.socket.close(linger=0)
        self.status = STATUS.ready

    def send(self, msg, topic=''):
        """
        Send a message to all subscribers of topic.

        Raises:
            See :func:`validate_topic_type` for list of possible Exceptions.

        """
        self.logger.debug('Trying to publish to topic "%s" (type: %s): %s' %
                          (topic, type(topic), msg))
        validate_topic_type(topic)
        self.socket.send_multipart([topic, msg])

    def receive(self):
        """
        """
        msg = self.socket.recv()
        return msg


class Subscriber(LoggerMixin):
    """
    Handles sockets for subscribing to messages
    """

    def __init__(self, bidirectional=False, context=None):
        """
        """
        super(Subscriber, self).__init__()
        self.context = context or zmq.Context.instance()
        self.socket_type = zmq.XSUB if bidirectional else zmq.SUB
        self.socket = self.context.socket(self.socket_type)
        self.status = STATUS.ready

        self.subscriptions = []

    def listen(self, addr="tcp://127.0.0.1:47330"):
        """
        """
        self.socket.bind(addr)
        self.status = STATUS.started

    def connect(self, addr="tcp://127.0.0.1:47331"):
        """
        Connects to a publisher at `addr`
        """
        self.socket.connect(addr)
        self.status = STATUS.started

    def close(self):
        """
        close the socket
        """
        self.status = STATUS.stopping
        self.socket.close(linger=0)
        self.status = STATUS.ready

    def subscribe(self, topic):
        """
        """
        if topic in self.subscriptions:
            return

        validate_topic_type(topic)
        self.socket.setsockopt(zmq.SUBSCRIBE, topic)
        self.subscriptions.append(topic)

    def unsubscribe(self, topic):
        """
        """
        validate_topic_type(topic)
        self.socket.setsockopt(zmq.UNSUBSCRIBE, topic)
        if topic in self.subscriptions:
            self.subscriptions.remove(topic)

    def receive(self):
        """
        """
        topic, msg = self.socket.recv_multipart()
        return topic, msg


class Poller(LoggerMixin):
    def __init__(self):
        super(Poller, self).__init__()
        self.zpoller = zmq.Poller()
        self.sockets = []

    def register(self, socket, flag=0):
        self.logger.debug('Registering %s with flag: %d' % (socket, flag))
        self.sockets.append(socket)
        self.zpoller.register(socket.socket, flag)

    def unregister(self, socket):
        self.logger.debug('Unregistering %s' % socket)
        self.sockets.remove(socket)
        self.zpoller.unregister(socket.socket)

    def poll(self, timeout=None):
        events = dict(self.zpoller.poll(timeout))
        ret_events = {}
        for s in self.sockets:
            if s.socket not in events:
                continue
            ret_events[s] = events[s.socket]

        return ret_events


class Switch(LoggerMixin):
    """
    The Message Broker. Think of this more as a switch or HTTP proxy

    .. note::
       If you're using this with inproc:// order matters. Don't try to connect
       PUB/SUB sockets to XPUB/XSUB without first listening on the X variant
       first.
    """
    def __init__(self, *args, **kwargs):
        super(Switch, self).__init__(*args, **kwargs)
        self.pub = Publisher(bidirectional=True)
        self.sub = Subscriber(bidirectional=True)
        self.status = STATUS.ready
        self.poller = Poller()

    def listen(self, pub_addr, sub_addr):
        """
        Starts listening on the provided addresses
        """
        self.pub.listen(pub_addr)
        self.sub.listen(sub_addr)
        self.status = STATUS.started

    def start(self, threading_model=None):
        """
        start switching messages using `threading_model` to process them. This
        is a blocking action.
        """
        self.poller.register(self.pub, zmq.POLLIN)
        self.poller.register(self.sub, zmq.POLLIN)

        while True:
            events = dict(self.poller.poll())

            if events.get(self.pub) == zmq.POLLIN:
                msg = self.pub.socket.recv()
                # if msg[0] == '\x01':
                #     self.logger.debug('Subscribe request: "%s"' % msg)
                # elif msg[0] == '\x00':
                #     self.logger.debug('Unsubscribe request: "%s"' % msg)

                self.sub.socket.send_multipart(msg)

            if events.get(self.sub) == zmq.POLLIN:
                topic, msg = self.sub.receive()
                # self.logger.debug('Received message on topic "%s": %s' %
                #                   (topic, msg))
                self.pub.send(msg, topic=topic)

    def stop(self):
        self.status = STATUS.stopping
        self.pub.close()
        self.sub.close()
        self.status = STATUS.ready

    @property
    def subscriptions(self):
        return list(self.sub.subscriptions)


def validate_topic_type(topic):
    """
    Validates the topic as the right type.

    .. warning::
       You are incharge of catching these exceptions and logging some
       meaningful output.

    Raises: `UnicodeError` `ValueError`
    """
    if isinstance(topic, unicode):
        raise UnicodeError('Topics must not be unicode')
    elif not isinstance(topic, VALID_TOPIC_TYPES):
        raise ValueError('Invalid Topic Type %s' % type(topic))
