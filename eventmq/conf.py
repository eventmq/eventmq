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

# Settings that should not be honored if they are defined in the global
# section. These must be lowercase.
_AMBIGUOUS_SETTINGS = ('frontend_listen_addr', 'backend_listen_addr',
                       'administrative_listen_addr', 'connect_addr',
                       'reconnect_timeout')

#: SUPER_DEBUG basically enables more debugging logs. Specifically the messages
#: at different levels in the application.
#: Default: False
SUPER_DEBUG = False

#: Don't show HEARTBEAT message when debug logging is enabled
#: Default: True
HIDE_HEARTBEAT_LOGS = True

# When a queue name isn't specified use this queue name for the default. It
# would be a good idea to have a handful of workers listening on this queue
# unless you're positive that everything specifies a queue with workers.
DEFAULT_QUEUE_NAME = 'default'

# Default queues for the Job Manager to listen on. The values here should match
# the values defined on the router.
QUEUES = [(10, DEFAULT_QUEUE_NAME), ]

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
FRONTEND_LISTEN_ADDR = 'tcp://127.0.0.1:47291'
BACKEND_LISTEN_ADDR = 'tcp://127.0.0.1:47290'
# Used to monitor and manage the devices
ADMINISTRATIVE_LISTEN_ADDR = 'tcp://127.0.0.1:47293'

# Where the router is located
CONNECT_ADDR = None

# # Where the Scheduler should connect.
CONNECT_ADDR_DEFAULT = 'tcp://127.0.0.1:47290'
CONNECT_ADDR_FAILOVER = 'tcp://127.0.0.1:47290'


# How many jobs should the job manager concurrently handle?
CONCURRENT_JOBS = 4
HWM = 10000

# Redis settings
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = ''

MAX_JOB_COUNT = 1024

# }}}
