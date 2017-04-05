#######################
Settings (eventmq.conf)
#######################
EventMQ uses a standard INI style config file found at ``/etc/eventmq.conf``.

******
Global
******

name
====
Default: <random uuid>

A unique name to give the node.

super_debug
===========
Default: False

When super debug is enabled it will print almost all message recieved and sent
(the exception is heartbeat message which is controlled by
hide_heartbeat_logs_). This is extremley verbose but very useful for debugging
communication errors. If you enables this setting make sure to enable some type
of log rotation to keep your disk usage reasonable.

hide_heartbeat_logs
===================
Default: True

By default the heartbeat message are hidden when super_debug_ is enabled. If you
want to also want to see heartbeats and their replies set this to False. This is
useful when debugging connection issues.

default_queue_name
==================
Default: default

The name of the default queue. When jobs don't specify the queue they should run
in this value is used.


.. note::

   It is recommended that you have some workers listening for jobs on your
   default queue so that anything that is not explicitly assigned will still be
   executed.

disable_heartbeats
==================
Default: False

Setting this to True will disable heartbeat checks all together. Heartbeats are
used to check the aliveness of peers. By disabling heartbeats you run the risk
of sending messages to peers who aren't in a state of accepting them.

heartbeat_liveness
==================
Default: 3

A node will assume it's peer is dead after this many missed heartbeat messages.


heartbeat_timeout
=================
Default: 5

Mark the heartbeat as missed after this many seconds. By multiplying this value
with heartbeat_liveness_ you get the number of total seconds it takes for a node
to mark the peer as dead

heartbeat_interval
==================
Default: 3

The number of seconds to wait between sending each heartbeat. It is important
that this number is less than heartbeat_timeout_ otherwise you will end up with
peers being marked as dead when they're not.

config_file
===========
Default: /etc/eventmq.conf

The location of the config file to read settings from.

******
Router
******

.. _router_frontend_listen_addr:

frontend_listen_addr
====================
Default: tcp://127.0.0.1:47291

The address the router should listen on for client and scheduler connections.

.. _router_backend_listen_addr:

backend_listen_addr
===================
Default: tcp://127.0.0.1:47290

The address the router should listen on for job manager connections.

.. _router_administrative_listen_addr:

administrative_listen_addr
==========================
Default: tcp://127.0.0.1:37293

The address the router should listen on for administrative commands.

hwm
===
Default: 10000

Hard limit of messages to store in memory before dropping them. When there are
no availabe slots the router will start buffering messages internally. Take into
account the amount of memory you have on the system and the average size of your
message when defining this value.

*********
Scheduler
*********

***********
Job Manager
***********

.. _jobmanager_connect_addr:

connect_addr
============
Default: tcp://127.0.0.1:47290

The address of the router's :ref:`router_backend_listen_addr` to connect to.

.. _jobmanager_queues:

queues
======
Default: [[10, "default"]]

Comma seperated list of queues to process jobs for with thier weights. This list
must be valid JSON otherwise an error will be thrown.
Example: ``queues=[[10, "data_process"], [15, "email"]]``.  With these
weights and the ``CONCURRENT_JOBS`` setting, you should be able to tune managers
running jobs locally efficiently. If you have a larger server with a weight of
50 on q1 and 8 concurrent jobs and a smaller server with a weight 30 and 4
concurrent jobs, the q1 jobs will be sent to the large box until it is no longer
accepting jobs. At this point jobs will start to be sent to the next highest
number until the large box is ready to accept another q1 job.

.. note::

   When defining queues on the command line the format is ``weight,queuename
   weight,queuename``. For example: ``-Q 10,default 15,high``

concurrent_jobs
===============
Default: 4

This is the number of concurrent jobs the indiviudal job manager should execute
at a time. If you are using the multiprocess or threading model this number
becomes important as you will want to control the load on your server. If the
load equals the number of cores on the server, processes will begin waiting for
cpu cycles and things will begin to slow down.

A safe number to choose if your jobs block a lot would be (2 * cores). If your
jobs are cpu intensive you will want to set this number to the number of cores
you have or (cores - 1) to leave cycles for the os and other processes. This is
something that will have to be tuned based on the jobs that are
running. Grouping similar jobs in named queues will help you tune this number.

Scheduler
*********

.. _scheduler_connect_addr:

connect_addr
============
Default: tcp://127.0.0.1:47291

The address of the router's :ref:`router_frontend_listen_addr` to connect to.

.. _scheduler_queues:

queues
======
Default: [[10, "default"]]

Comma seperated list of queues to process jobs for with thier weights. This list
must be valid JSON otherwise an error will be thrown.
Example: ``queues=[[10, "data_process"], [15, "email"]]``.  With these
weights and the ``CONCURRENT_JOBS`` setting, you should be able to tune managers
running jobs locally efficiently. If you have a larger server with a weight of
50 on q1 and 8 concurrent jobs and a smaller server with a weight 30 and 4
concurrent jobs, the q1 jobs will be sent to the large box until it is no longer
accepting jobs. At this point jobs will start to be sent to the next highest
number until the large box is ready to accept another q1 job.

.. note::

   When defining queues on the command line the format is ``weight,queuename
   weight,queuename``. For example: ``-Q 10,default 15,high``

redis_host
==========
Default: localhost

Hostname for the Redis server. The scheduler uses redis to persist scheduled
jobs.

redis_port
==========
Default: 6379

Port for the Redis server

redis_db
========
Default: 0

Redis database to store schedules

redis_password
==============
Default: <empty string>

Password to the Redis server

Publisher
*********

.. _publisher_frontend_listen_addr:

frontend_listen_addr
====================
Default: tcp://127.0.0.1:47298

The address the publisher should listen on for client connections.

.. _publisher_backend_listen_addr:

backend_listen_addr
===================
Default: tcp://127.0.0.1:47299

The address the publisher should listen on for subscribers.
