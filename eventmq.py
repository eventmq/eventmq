"""
eventmq
"""
import logging
import zmq
Context = zmq.Context

LOG_LEVEL = logging.DEBUG
PROTOCOLS = ('tcp', 'udp', 'pgm', 'epgm', 'inproc', 'ipc')
VALID_TOPIC_TYPES = (int, str)


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


class Publisher(LoggerMixin):
    """
    Handles sockets for publishing messages

    Attrs:
        context (:class:`zmq.Context`)
        socket_type
        socket (:class:`zmq.Socket`): raw zmq socket
    """

    def __init__(self, xpub=False, context=None):
        """
        """
        super(Publisher, self).__init__()
        self.context = context or zmq.Context.instance()
        self.socket_type = zmq.XPUB if xpub else zmq.PUB
        self.socket = self.context.socket(self.socket_type)
        self.status = STATUS.ready

    def listen(self, addr="tcp://127.0.0.1:47331"):
        """
        Starts listening on an address
        """
        self.socket.bind(addr)
        self.status = STATUS.started

    def close(self):
        """
        Close the socket
        """
        self.socket.close()
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


class Subscriber(LoggerMixin):
    """
    Handles sockets for subscribing to messages
    """

    def __init__(self, xsub=False, context=None):
        """
        """
        super(Subscriber, self).__init__()
        self.context = context or zmq.Context.instance()
        self.socket_type = zmq.XSUB if xsub else zmq.SUB
        self.socket = self.context.socket(self.socket_type)
        self.status = STATUS.ready

        self.subscriptions = ()

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
        self.socket.close()
        self.status = STATUS.ready

    def subscribe(self, topic):
        """
        """
        validate_topic_type(topic)
        self.socket.setsockopt(zmq.SUBSCRIBE, topic)

    def unsubscribe(self, topic):
        """
        """
        validate_topic_type(topic)
        self.socket.setsockopt(zmq.UNSUBSCRIBE, topic)

    def receive(self):
        """
        """
        topic, msg = self.socket.recv_multipart()
        return topic, msg


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
