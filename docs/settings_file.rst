##############################
Server Settings (eventmq.conf)
##############################
EventMQ uses a standard INI style config file with the default
location of ``/etc/eventmq.conf``. If you would like to specify a custom path
you can use the ``EVENTMQ_CONFIG_FILE`` environment variable.

All of these options can be defined via environment variables by converting
them to upper case and prefixing them with ``EVENTMQ_``. For example
``EVENTMQ_MAX_SOCKETS=2048``.

******
Global
******

super_debug
===========
Default: False

Enable most verbose level of debug statements

max_sockets
===========
Default: 1024

Define the max sockets for a process/context

******
Router
******

frontend_addr
=============
Default: 'tcp://127.0.0.1:47291'

The address used to listen for client and scheduler connections


backend_addr
============
Default: 'tcp://127.0.0.1:47291'

The address used to listen for connections from workers

wal
===
Default: '/var/log/eventmq/wal.log'

Write-ahead Log for replaying messages received by the Router.  Will
try to create the directory specified and append to the filename given.
Requires correct permissions to write to the given file.

wal_enabled
===========
Default: False

Enable or disable the Write-ahead Log

*********
Scheduler
*********

scheduler_addr
==============
Default: 'tcp://127.0.0.1:47291'

The address the scheduler will use to connect to the broker

rq_host
=======
Default: 'localhost'

The hostname of the redis server used to persist scheduled jobs.  This is
expected to support redis' save operation which saves the contents to disk.

rq_port
=======
Default: 6379

Port of redis server to connect to.

rq_db
=====
Default: 0

Which redis database to use

rq_password
===========
Default: ''

Password to use when connecting to redis

***********
Job Manager
***********

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

queues
======
Default: [[10, "default"]]

Comma seperated list of queues to process jobs for with their weights. This list
must be valid JSON otherwise an error will be thrown.
Example: ``queues=[[10, "data_process"], [15, "email"]]``.  With these
weights and the ``CONCURRENT_JOBS`` setting, you should be able to tune managers
running jobs locally efficiently. If you have a larger server with a weight of
50 on q1 and 8 concurrent jobs and a smaller server with a weight 30 and 4
concurrent jobs, the q1 jobs will be sent to the large box until it is no longer
accepting jobs. At this point jobs will start to be sent to the next highest
number until the large box is ready to accept another q1 job.

.. note::

   It is recommended that you have some workers listening for jobs on your
   default queue so that anything that is not explicitly assigned will still be
   run.

setup_callabe/setup_path
========================
Default: '' (Signifies no task will be attempted)

Strings containing path and callable to be run when a worker is spawned
if applicable to that type of worker.  Currently the only supported worker is a
MultiProcessWorker, and is useful for pulling any global state into memory.

max_job_count
=============
Default: 1024

After a worker runs this amount of jobs, it will gracefully exit and be replaced
