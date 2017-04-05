#########
CHANGELOG
#########
For upcoming releases see the milestone page: https://github.com/eventmq/eventmq/milestones

0.4
===
* Update config file loading. Each device will load the ``global`` section followed by it's own section of the provided config (router for emq-router, jobmanager for emq-jobmanager, etc)
* Allow configurations options to be set via command-line.

Backwards incompatible changes
------------------------------
* ``emq-pubsub`` has been renamed to ``emq-publisher``
* Configuration: The name and section for the listening addresses has changed:
   * Job Manager: ``WORKER_ADDR`` is now ``CONNECT_ADDR`` in the ``[jobmanager]`` section
   * Job Manager: ``WORKER_ADDR_DEFAULT`` is now ``CONNECT_ADDR_DEFAULT`` in the ``[jobmanager]`` section
   * Job Manager: ``WORKER_ADDR_FAILOVER`` is now ``CONNECT_ADDR_FAILOVER`` in the ``[jobmanager]`` section
   * Publisher: ``PUBLISHER_FRONTEND_ADDR`` is not ``FRONTEND_LISTEN_ADDR`` in the ``[publisher]`` section
   * Publisher: ``PUBLISHER_BACKEND_ADDR`` is not ``BACKEND_LISTEN_ADDR`` in the ``[publisher]`` section
   * Router: ``FRONTEND_ADDR`` is now ``FRONTEND_LISTEN_ADDR`` in the ``[router]`` section
   * Router: ``BACKEND_ADDR`` is now ``BACKEND_LISTEN_ADDR`` in the ``[router]`` section
   * Scheduler: ``SCHEDULER_ADDR`` is now ``CONNECT_ADDR`` in the ``[scheduler]`` section
   * ``RQ_HOST`` is now ``REDIS_HOST``
   * ``RQ_PORT`` is now ``REDIS_PORT``
   * ``RQ_DB`` is now ``REDIS_DB``
   * ``RQ_PASSWORD`` is now ``REDIS_PASSWORD``
   * ``ADMINISTRATIVE_ADDR`` is now ``ADMINISTRATIVE_LISTEN_ADDR`` in each respective section
   * ``CONNECT_ADDR_DEFAULT`` has been removed from the ``[jobmanager]`` section. HA will be retooled and added back in the High Availability Milestone.
   * ``CONNECT_ADDR_FAILOVER`` has been removed from the ``[jobmanager]`` section. HA will be retooled and added back in the High Availability Milestone.
* Configuration: Command line options for emq-jobmanager
   * ``--broker-addr`` ``-B`` has been renamed to ``--connect-addr`` ``-A``
   * ``--jobs`` has been renamed to ``--concurrent-jobs``
