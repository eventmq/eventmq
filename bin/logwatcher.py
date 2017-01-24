#!/usr/bin/env python

import zmq

s = zmq.Context.instance().socket(zmq.SUB)
s.setsockopt(zmq.SUBSCRIBE, '')
s.connect('tcp://127.0.0.1:33445')

poller = zmq.Poller()
poller.register(s, zmq.POLLIN)
while True:
    events = dict(poller.poll())

    if events.get(s) == zmq.POLLIN:
        msg = s.recv_multipart()
        print msg  # noqa
