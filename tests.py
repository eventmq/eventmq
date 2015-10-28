#!/usr/bin/env python
import gc
import logging
import unittest

import zmq
import eventmq

INPROC = "inproc://derp"


class PublisherTestCase(unittest.TestCase):
    def setUp(self):
        self._zcontext = zmq.Context.instance()

        self.publisher = eventmq.Publisher()
        self.publisher.logger.setLevel(logging.WARNING)
        if self.publisher.status == eventmq.STATUS.ready:
            self.publisher.listen(INPROC)

        self.zsubscriber = self._zcontext.socket(zmq.SUB)
        self.zsubscriber.connect(INPROC)

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
        if self.zsubscriber.poll(500):
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
        self.publisher.close()
        if self.zsubscriber:
            self.zsubscriber.close()

        self.publisher = None
        self.zsubscriber = None
        gc.collect()


class SubscriberTestCase(unittest.TestCase):
    TOPIC = 'derp topic'
    uTOPIC = u'derp topic'
    MSG = 'batton down the hatchs'
    uMSG = u'swab the poop deck'

    def setUp(self):
        self._zcontext = zmq.Context.instance()
        self.subscriber = eventmq.Subscriber()
        self.subscriber.logger.setLevel(logging.WARNING)

        self.zpublisher = self._zcontext.socket(zmq.PUB)
        self.zpublisher.bind(INPROC)

        if self.subscriber.status == eventmq.STATUS.ready:
            self.subscriber.connect(INPROC)

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
        self.subscriber.close()
        self.zpublisher.close(linger=0)

        self.subscriber = None
        self.zpublisher = None
        gc.collect()


if __name__ == "__main__":
    unittest.main()
