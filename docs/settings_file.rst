########
Settings
########
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

queues
======
Default: default

Comma seperated list of queues to process jobs for. Example:
``queues=high,med,low,default``. The philosophy taken for this list is each job
manager should have a single primary queue. This queue is the first in the list
(in the case of the example ``high`` is the primary queue). Subsequent queues
are queues that this job manager should help out with should jobs be backed up,
and there are no primary queue jobs to take care of.

.. note::

   It is recommended that you have some workers listening for jobs on your
   default queue so that anything that is not explicitly assigned will still be
   run.
