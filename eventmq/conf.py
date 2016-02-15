# SUPER_DEBUG basically enables more debugging logs. specifically of messages
# at different levels in the application
SUPER_DEBUG = True
# Don't show HEARTBEAT message when debug logging is enabled
HIDE_HEARTBEAT_LOGS = True

# When a queue name isn't specified use this queue name for the default. It
# would be a good idea to have a handful of workers listening on this queue
# unless you're positive that everything specifies a queue with workers.
DEFAULT_QUEUE_NAME = 'default'

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
# Default addresses to localhost
FRONTEND_ADDR = 'tcp://127.0.0.1:47291'
BACKEND_ADDR = 'tcp://127.0.0.1:47290'
SCHEDULER_ADDR = 'tcp://127.0.0.1:47291'
WORKER_ADDR = 'tcp://127.0.0.1:47290'

# Redis settings
RQ_HOST = 'localhost'
RQ_PORT = 6379
RQ_DB = 0
RQ_PASSWORD = ''
WORKERS = 4
HWM = 10000
# }}}
