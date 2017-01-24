##############################
Server Settings (eventmq.conf)
##############################
EventMQ uses a standard INI style config file found at ``/etc/eventmq.conf``.

******
Router
******

*********
Scheduler
*********

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
Default: (10, default)

Semi-colon seperated list of queues to process jobs for with thier
weights. Example: ``queues=(10, data_process); (15, email)``.  With these
weights and the ``CONCURRENT_JOBS`` setting, you should be able to tune managers
running jobs locally pretty efficiently. If you have a larger box with a weight
of 50 on q1 and 8 concurrent jobs and a smaller box with a weight 30 and 4
concurrent jobs, the q1 jobs will be sent to the large box until it is no longer
accepting jobs. At this point jobs will start to be sent to the next highest
number until the large box is ready to accept another q1 job.

.. note::

   It is recommended that you have some workers listening for jobs on your
   default queue so that anything that is not explicitly assigned will still be
   run.
