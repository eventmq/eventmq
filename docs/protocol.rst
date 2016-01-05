******************************
EventMQ Protocol Specification
******************************
*The status of this document is alpha and subject to heavy change*

Goals
=====
The EventMQ Protocol (eMQP) defines a reliable service-oriented request-reply and pub-sub dialog between a set of clients, a broker, and a set of workers. This goal is to

The goals are to:

 * Specify a protocol to follow when implementing a component to EventMQ.
 * Allow requests to be routed to workers by an abstracted service name.
 * Detect disconnected peers through heartbeating.
 * Allow for message tracing and debugging.


License
=======
This Specification is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This Specification is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

Language
========
The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC 2119[[1](http://tools.ietf.org/html/rfc2119)].

Architecture
============
insert pretty picture here

Topology
--------
eMQP connects a set of client applications (e.g. web servers), a broker, and a pool of workers. Clients connect to the broker as well as the workers.

'Clients' is defined as application issuing requests and 'workers' as applications that process these requests. (Workers consist of a `JobManager` and a pool of `Worker` resources where the job executes.)

The EventMQ broker handles a set of named queues. The broker SHOULD serve clients on a fair request and MAY deliver requests to workers on any basis, including 0MQ's built-in round robin or least-recently used.

ROUTER Addressing
-----------------
In the case of request-reply, the broker MUST use a ROUTER socket to accept requests from both clients and workers. The broker MAY use a seperate socket implementing a subset of eMQP, or MAY use a single socket implementing all of eMQP.

From the 0MQ manual[[2](http://api.zeromq.org/master:zmq-socket)]
> When receiving messages a ROUTER socket shall prepend a message part containing the identity of the originating peer to the message before passing it to the application. When sending messages a ROUTER socket shall remove the first part of the message and use it to determine the identity of the peer the message shall be routed to.

This extra frame is not shown in the specifications below.

Global Frames
-------------
An **ACK** command consists of a 4-frame multipart message, formatted as follows.

====== ============== ===========
FRAME  Value          Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      ACK            command
3      _MSGID_        A unique id for the msg
4      _MSGID_        The message id of the message this ACK is acknowledging
====== ============== ===========

eMQP / Client
-------------
A **REQUEST** command consists of a 7-frame multipart message, formatted as follows.

====== ============== ===========
FRAME  Value          Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      REQUEST        command
3      _MSGID_        A unique id for the msg
4      _QUEUE_NAME_   the name of the queue the worker belongs to
5      _HEADERS_      dictionary of headers. can be an empty set
6      _MSG_          The message to send
====== ============== ===========

A **PUBLISH** command consists of a 7-frame multipart messag, formatted as follows.

====== ============== ===========
FRAME  Value          Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      PUBLISH        command
3      _MSGID_        A unique id for the msg
4      _TOPIC_NAME_   the name of the queue the worker belongs to
5      _HEADERS_      csv list of headers
6      _MSG_          The message to send
====== ============== ===========

A **SCHEDULE** command consists of a 7-frame multipart message, formatted as follows.

====== ============== ===========
FRAME   Value         Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      SCHEDULE       command
3      _MSGID_        A unique id for the msg
4      _TOPIC_NAME_   name of queue that the job should run in
5      _HEADERS_      csv list of headers for this message
6      _MSG_          The message to send
====== ============== ===========

eMQP / Scheduler
----------------
An **INFORM** command consists of a 6-frame multipart message, formatted as follows.

====== ============== ===========
FRAME   Value         Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      INFORM         command
3      _MSGID_        A unique id for the msg
4      _QUEUE_NAME_   csv seperated names of queue the worker belongs to
5      scheduler      type of peer connecting
====== ============== ===========

eMQP / Worker
-------------
An **INFORM** command consists of a 5-frame multipart message, formatted as follows.

====== ============== ===========
FRAME   Value         Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      INFORM         command
3      _MSGID_        A unique id for the msg
4      _QUEUE_NAME_   csv seperated names of queue the worker belongs to
5      worker         type of peer connecting
====== ============== ===========

A **READY** frame consists of a 4-frame multipart message, formatted as follows.

====== ============== ===========
FRAME  Value          Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      READY          command
3      _MSGID_        A unique id for the msg
====== ============== ===========

A **REPLY** frame consists of a 5-frame multipart message, formatted as follows.

====== ============== ===========
FRAME  Value          Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      REPLY          command
3      _MSGID_        A unique id for the msg
4      _MSG_          The reply to respond with
====== ============== ===========

A **HEARTBEAT** frame consists of a

====== ============== ===========
FRAME  Value          Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      HEARTBEAT      command
3      _MSGID_        A unique id for the msg
4      _UNIX_TS_      A unix timestamp
====== ============== ===========

A **DISCONNECT** frame consists of

====== ============== ===========
FRAME  Value          Description
====== ============== ===========
0      _EMPTY_        leave empty
1      eMQP/1.0       Protocol version
2      DISCONNECT     command
3      _MSGID_        A unique id for the msg
====== ============== ===========

Heartbeating
------------
 * HEARTBEAT commands are valid at any time after an INFORM command
 * Any command except DISCONNECT act as a heartbeat. Peers SHOULD NOT send HEARTBEAT commands while sending other commands.
 * Worker and broker MUST send heartbeats at regular and agreed-upon intervals.
 * Scheduler and broker MUST send heartbeats at regular and agreed-upon intervals.
 * If the worker detects that the broker disconnected it SHOULD restart the conversation.
 * If the broker detects that a worker has disconnected it should stop sending it a message of any type.
 * If the scheduler detects that the broker disconnects it SHOULD restart the conversation.
 * If the broker detects that a scheduler has disconnected it should ??????????.

REQUEST Headers
---------------
Headers MUST be 0 to many comma seperated values inserted into the header field. If there are no headers requried, send an empty string MUST be sent where headers are required.

Below is a table which defines and describes the headers.

=============== ======= ======= ======= ===========
Header          REQUEST PUBLISH Default Description
=============== ======= ======= ======= ===========
reply-requested X               False   Once the job is finished, send a reply back with information from the job. If there is no information reply with a True value.
retry-count:#   X               0       Retry a failed job this many times before accepting defeat.
guarantee       X               False   Ensure the job completes by letting someone else worry about a success reply.
=============== ======= ======= ======= ===========
