Goals
-----
The EventMQ Protocol (eMQP) defines a reliable service-oriented request-reply and pub-sub dialog between a set of clients, a broker, and a set of workers. This goal is to 

The goals are to:

 * Specify a protocol to follow when implementing a component to EventMQ.
 * Allow requests to be routed to workers by an abstracted service name.
 * Detect disconnected peers through heartbeating.
 * Define scaling strategies.
 * Allow for message tracing and debugging.
 

License
-------
This Specification is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This Specification is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

Language
--------
The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC 2119[[1](http://tools.ietf.org/html/rfc2119)].

Architecture
------------
_insert pretty picture here_

### Topology
eMQP connects a set of client applications (e.g. web servers), a broker, and a pool of workers. Clients connect to the broker as well as the workers.

'Clients' is defined as application issuing requests and 'workers' as applications that process these requests. (Workers consist of a `JobManager` and a pool of `Worker` resources where the job executes.)

The EventMQ broker handles a set of named queues. The broker SHOULD serve clients on a fair request and MAY deliver requests to workers on any basis, including 0MQ's built-in round robin or least-recently used.

### ROUTER Addressing
In the case of request-reply, the broker MUST use a ROUTER socket to accept requests from both clients and workers. The broker MAY use a seperate socket implementing a subset of eMQP, or MAY use a single socket implementing all of eMQP.

From the 0MQ manual[[2](http://api.zeromq.org/master:zmq-socket)]
> When receiving messages a ROUTER socket shall prepend a message part containing the identity of the originating peer to the message before passing it to the application. When sending messages a ROUTER socket shall remove the first part of the message and use it to determine the identity of the peer the message shall be routed to.

This extra frame is not shown in the specifications below.

### eMQP / Client
A **REQUEST** frame 

FRAME | Value         | Description
:----:|---------------|------------
0     | _EMPTY_       | leave empty
1     | eMQP/1.0      | Protocol version
2     | READY         | 
3     | _MSGID_       | A unique id for the msg
4     | _QUEUE_NAME_  | the name of the queue the worker belongs to

### eMQP / Worker
An **INFORM** command consists of a 5-frame multipart message, formatted as follows.

FRAME | Value         | Description
:----:|---------------|------------
0     | _EMPTY_       | leave empty
1     | eMQP/1.0      | Protocol version
2     | INFORM        |
3     | _MSGID_       | A unique id for the msg
4     | _QUEUE_NAME_  | the name of the queue the worker belongs to

A **READY** frame consists of a 4-frame multipart message, formatted as follows.

FRAME | Value         | Description
:----:|---------------|------------
0     | _EMPTY_       | leave empty
1     | eMQP/1.0      | Protocol version
2     | READY         |
3     | _MSGID_       | A unique id for the msg