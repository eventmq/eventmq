import logging
import unittest

import zmq
import eventmq

INPROC = "inproc://derp"


class PublisherTestCase(unittest.TestCase):
    def setUp(self):
        self._zcontext = zmq.Context.instance()
        self._zmq_subscriber = None
        self._publisher = eventmq.Publisher()
        self._publisher.logger.setLevel(logging.WARNING)

    @property
    def publisher(self):
        """
        property for `self._publisher` so testing `Publisher.__init__` doesn't
        clog the INPROC tube.
        """
        if self._publisher.status == eventmq.STATUS.ready:
            self._publisher.listen(INPROC)
        return self._publisher

    @property
    def zsubscriber(self):
        """
        Your very own zmq subscriber socket
        """
        if not self._zmq_subscriber:
            self._zmq_subscriber = self._zcontext.socket(zmq.SUB)
            self._zmq_subscriber.connect(INPROC)

        return self._zmq_subscriber

    def test_default_pub_creation(self):
        publisher = eventmq.Publisher()
        self.assertEqual(publisher.socket.socket_type, zmq.PUB)

    def test_xpub_creation(self):
        publisher = eventmq.Publisher(xpub=True)
        self.assertEqual(publisher.socket.socket_type, zmq.XPUB)

    def test_send_unicode(self):
        test_message = 'testmsg'

        self.zsubscriber.setsockopt(zmq.SUBSCRIBE, '')
        self.publisher.send(test_message)
        topic, msg = self.zsubscriber.recv_multipart()

        self.assertEqual(msg, test_message)

    def test_valueerror_weird_topic(self):
        self.assertRaises(ValueError,
                          self.publisher.send,
                          'asdf',
                          topic=3924.34)

    def test_unicodeerror_topic(self):
        self.assertRaises(UnicodeError,
                          self.publisher.send,
                          'asdf',
                          topic=u'a')

    def tearDown(self):
        if self._publisher:
            self._publisher.close()
        if self._zmq_subscriber:
            self._zmq_subscriber.close()


class SubscriberTestCase(unittest.TestCase):
    TOPIC = 'derp topic'
    uTOPIC = u'derp topic'
    MSG = 'batton down the hatchs'
    uMSG = u'swab the poop deck'

    def setUp(self):
        self._zcontext = zmq.Context.instance()
        self._zmq_publisher = None
        self._subscriber = eventmq.Subscriber()
        self._subscriber.logger.setLevel(logging.WARNING)

    @property
    def subscriber(self):
        """
        property for `self._subscriber` so testing `Subscriber`.__init__
        doesn't clog the INPROC tubes.

        Returns: :class:`Subscriber`
        """
        if self._subscriber.status == eventmq.STATUS.ready:
            self._subscriber.connect(INPROC)
        return self._subscriber

    @property
    def zpublisher(self):
        """
        Your very own zmq publisher socket
        """
        if not self._zmq_publisher:
            self._zmq_publisher = self._zcontext.socket(zmq.PUB)
            self._zmq_publisher.bind(INPROC)
        return self._zmq_publisher

    def test_default_sub_creation(self):
        subscriber = eventmq.Subscriber()
        self.assertEqual(subscriber.socket.socket_type, zmq.SUB)

    def test_xsub_creation(self):
        subscriber = eventmq.Subscriber(xsub=True)
        self.assertEqual(subscriber.socket.socket_type, zmq.XSUB)

    def test_subscribe(self):
        self.subscriber.subscribe(self.TOPIC)

        self.zpublisher.send_multipart([self.TOPIC, self.MSG])

        topic, msg = self.subscriber.receive()

        self.assertEqual(topic, self.TOPIC)
        self.assertEqual(msg, self.MSG)

    def test_unsubscribe(self):
        self.subscriber.socket.setsockopt(zmq.SUBSCRIBE, self.TOPIC)

        self.subscriber.unsubscribe(self.TOPIC)
        self.zpublisher.send_multipart([self.TOPIC, self.MSG])

        self.assertEqual(self.subscriber.socket.poll(100), 0)

    def test_unicodeerror_unicode_topic(self):
        self.assertRaises(UnicodeError, self.subscriber.subscribe, self.uTOPIC)

    def test_valueerror_non_str_int_topic(self):
        self.assertRaises(ValueError, self.subscriber.subscribe, 1.39)
        self.assertRaises(ValueError, self.subscriber.subscribe, list('v'))

    def tearDown(self):
        if self._subscriber:
            self._subscriber.close()
        if self._zmq_publisher:
            self._zmq_publisher.close()


if __name__ == "__main__":
    unittest.main()
