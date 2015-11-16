#!/usr/bin/env python
import gc
import unittest

import zmq
import eventmq, router

INPROC = "inproc://derp"


class PublisherTestCase(unittest.TestCase):
    def setUp(self):
        self._zcontext = zmq.Context.instance()

        self.publisher = eventmq.Publisher()
        if self.publisher.status == eventmq.STATUS.ready:
            self.publisher.listen(INPROC)

        self.zsubscriber = self._zcontext.socket(zmq.SUB)
        self.zsubscriber.connect(INPROC)

    def test_default_pub_creation(self):
        publisher = eventmq.Publisher()
        self.assertEqual(publisher.socket.socket_type, zmq.PUB)

    def test_xpub_creation(self):
        publisher = eventmq.Publisher(bidirectional=True)
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

        self.zpublisher = self._zcontext.socket(zmq.PUB)
        self.zpublisher.bind(INPROC)

        if self.subscriber.status == eventmq.STATUS.ready:
            self.subscriber.connect(INPROC)

    def test_default_sub_creation(self):
        subscriber = eventmq.Subscriber()
        self.assertEqual(subscriber.socket.socket_type, zmq.SUB)

    def test_xsub_creation(self):
        subscriber = eventmq.Subscriber(bidirectional=True)
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

    def test_listen(self):
        TEMP_INPROC = 'inproc://test_listen'
        s = eventmq.Subscriber()
        s.subscribe('')
        s.listen(TEMP_INPROC)

        p = zmq.Context.instance().socket(zmq.PUB)
        p.connect(TEMP_INPROC)

        # Poll the subscription so we don't miss the first message. Slow
        # joiner syndrom
        s.socket.poll(1)  # TODO: Sync these two some how

        p.send('asdf')
        self.assert_(s.socket.poll() != 0)
        #topic, msg = s.socket.recv_multipart()
        #self.assertEqual(msg, 'asdf')

        s.close()
        p.close(linger=0)

    def tearDown(self):
        self.subscriber.close()
        self.zpublisher.close(linger=0)

        self.subscriber = None
        self.zpublisher = None
        gc.collect()


class RouterTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_init(self):
        r = router.Router()

        self.assertEqual(r.status, router.STATUS.ready)
        self.assertNotEqual(r.socket_ctx, None)
        self.assertNotEqual(r.incoming, None)
        self.assertNotEqual(r.outgoing, None)

    def test_same_ctx_instance(self):
        ctx = zmq.Context.instance()
        r = router.Router()

        self.assertEqual(r.socket_ctx, ctx)

    def test_listen(self):
        ctx = zmq.Context.instance()
        FE_ADDR = 'ipc://incoming'
        BE_ADDR = 'ipc://outgoing'

        r = router.Router()
        r.listen(frontend_addr=FE_ADDR, backend_addr=BE_ADDR)

        self.assertEqual(r.status, router.STATUS.started)

        req_sock = ctx.socket(zmq.REQ)
        rep_sock = ctx.socket(zmq.REP)

        req_sock.connect(FE_ADDR)
        rep_sock.connect(BE_ADDR)

if __name__ == "__main__":
    unittest.main()
