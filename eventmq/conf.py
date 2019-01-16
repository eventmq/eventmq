# This file is part of eventmq.
#
# eventmq is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 2.1 of the License, or (at your option)
# any later version.
#
# eventmq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with eventmq.  If not, see <http://www.gnu.org/licenses/>.
"""
:mod:`conf` -- Settings Definitions
===================================
"""

#: SUPER_DEBUG basically enables more debugging logs. Specifically the messages
#: at different levels in the application.
#: Default: False
SUPER_DEBUG = False

#: Don't show HEARTBEAT message when debug logging is enabled
#: Default: True
HIDE_HEARTBEAT_LOGS = True

#: The maximum number of sockets to open per-process/context
MAX_SOCKETS = 1024

# When a queue name isn't specified use this queue name for the default. It
# would be a good idea to have a handful of workers listening on this queue
# unless you're positive that everything specifies a queue with workers.
DEFAULT_QUEUE_NAME = 'default'
DEFAULT_QUEUE_WEIGHT = 10

# Default queues for the Job Manager to listen on. The values here should match
# the values defined on the router.
QUEUES = [(DEFAULT_QUEUE_WEIGHT, DEFAULT_QUEUE_NAME), ]

# {{{Job Manager
# How long should we wait before retrying to connect to a broker?
RECONNECT_TIMEOUT = 5  # in seconds

# Don't bother with HEARTBEATS, both sending and paying attention to them
DISABLE_HEARTBEATS = False
# Assume the peer is dead after this many missed heartbeats
HEARTBEAT_LIVENESS = 3
# Assume a missed heartbeat after this many seconds
HEARTBEAT_TIMEOUT = 5
# How often should a heartbeat be sent? This should be lower than
# HEARTBEAT_TIMEOUT for the broker
HEARTBEAT_INTERVAL = 3

# Default configuration file
CONFIG_FILE = '/etc/eventmq.conf'

# Default character encoding for strings in messages See these URLs for
# supported encodings:
# https://docs.python.org/2/library/codecs.html#standard-encodings
# https://docs.python.org/3/library/codecs.html#standard-encodings
DEFAULT_ENCODING = 'utf-8'

# Default addresses to localhost
# Router:
FRONTEND_ADDR = 'tcp://127.0.0.1:47291'
BACKEND_ADDR = 'tcp://127.0.0.1:47290'
# Where the Scheduler should connect.
SCHEDULER_ADDR = 'tcp://127.0.0.1:47291'
# Where the worker should connect
WORKER_ADDR = 'tcp://127.0.0.1:47290'
WORKER_ADDR_DEFAULT = 'tcp://127.0.0.1:47290'
WORKER_ADDR_FAILOVER = 'tcp://127.0.0.1:47290'
# Used to monitor and manage the devices
ADMINISTRATIVE_ADDR = 'tcp://127.0.0.1:47293'

# PubSub
PUBLISHER_INCOMING_ADDR = 'tcp://127.0.0.1:47298'
PUBLISHER_OUTGOING_ADDR = 'tcp://127.0.0.1:47299'

# How many jobs should the job manager concurrently handle?
CONCURRENT_JOBS = 4
HWM = 10000

# Redis settings
RQ_HOST = 'localhost'
RQ_PORT = 6379
RQ_DB = 0
RQ_PASSWORD = ''
REDIS_CLIENT_CLASS = 'redis.StrictRedis'
REDIS_CLIENT_CLASS_KWARGS = {}
REDIS_STARTUP_ERROR_HARD_KILL = True

MAX_JOB_COUNT = 1024

# Path/Callable to run on start of a worker process
# These options are deprecated for the more user-friendly
# SUBPROCESS_SETUP_FUNC which can be a full path to a function.
SETUP_PATH = ''
SETUP_CALLABLE = ''

# Function to run on the start of a new worker subprocess
SUBPROCESS_SETUP_FUNC = ''

# function to be run before the execution of every job
JOB_ENTRY_FUNC = ''
# function to be run after the execution of every job
JOB_EXIT_FUNC = ''
# Time to wait after receiving SIGTERM to kill the workers in the jobmanager
# forecfully
KILL_GRACE_PERIOD = 300
GLOBAL_TIMEOUT = 300

WAL = '/var/log/eventmq/wal.log'
WAL_ENABLED = False

# }}}
