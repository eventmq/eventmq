EventMQ
=======
[![CircleCI](https://circleci.com/gh/eventmq/eventmq.svg?style=svg)](https://circleci.com/gh/eventmq/eventmq)
[![Coverage Status](https://coveralls.io/repos/github/eventmq/eventmq/badge.svg)](https://coveralls.io/github/eventmq/eventmq)

# Overview
EventMQ is a message passing system focused on asynchronous job execution for Python built on [ZeroMQ](https://zeromq.org)

# Installation

EventMQ is available on PyPi:

```
pip install eventmq
```

# Support
## Documenation

[Documentation](https://eventmq.github.io/eventmq/)

## IRC

 #eventmq on [irc.freenode.net](https://webchat.freenode.net/?channels=#eventmq)

# Quick Start

my_jerbs.py
``` python
# Note: this file needs to be importable, so you should put it in your python path somewhere.
from eventmq import job

@job
def say_hello():
    print 'hello'
```

Terminal 1:

```
% emq-router
eventmq.utils.settings  WARNING - Config file at /etc/eventmq.conf not found. Continuing with defaults.
eventmq.receiver  INFO - Receiver 83cdb797-8f99-4b06-862c-06f6b6f5f6f6: Listening on tcp://127.0.0.1:47291
eventmq.receiver  INFO - Receiver 8e9b3b83-711c-4736-8efc-2a870e800245: Listening on tcp://127.0.0.1:47290
eventmq.receiver  INFO - Receiver f67d673f-c44a-4380-b289-d88e264af5cd: Listening on tcp://127.0.0.1:47293
eventmq.router  INFO - Listening for requests on tcp://127.0.0.1:47291
eventmq.router  INFO - Listening for workers on tcp://127.0.0.1:47290
eventmq.router  INFO - Listening for administrative commands on tcp://127.0.0.1:47293
```

Terminal 2

```
% emq-jobmanager
eventmq.utils.settings  WARNING - Config file at /etc/eventmq.conf not found. Continuing with defaults.
eventmq.utils.settings  WARNING - Config file at /etc/eventmq.conf not found. Continuing with defaults.
eventmq.sender  DEBUG - Connecting to tcp://127.0.0.1:47290
eventmq.utils.classes  INFO - Received ACK for router (or client) 9c7c3d5f-80f6-454a-b308-3231d1ce93b2
eventmq.utils.classes  INFO - Starting event loop...
```

Terminal 3

```
% EMQ_BROKER_ADDR=tcp://127.0.0.1:47291 PYTHONPATH=`pwd` python
Python 2.7.12 (default, Sep 29 2016, 13:30:34)
[GCC 6.2.1 20160916 (Red Hat 6.2.1-2)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
>>> from my_jerbs import say_hello
>>> say_hello.delay()
'a69cbbe5-589a-495a-b4bc-fe1645439d39'
```

You should see "hello" printed in Terminal 2
