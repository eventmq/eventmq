class STATUS(object):
    wtf = -1          # Something went wrong
    ready = 100       # Waiting to connect or listen
    starting = 101    # Starting to bind
    listening = 102   # bound
    connecting = 200
    connected = 201
    stopping = 300
    stopped = 301
    running = 400     # Running and accepting jobs actively


class CLIENT_TYPE(object):
    worker = 'worker'
    scheduler = 'scheduler'


# See doc/protocol.rst
PROTOCOL_VERSION = 'eMQP/1.0'

# PROTOCOL COMMANDS
DISCONNECT = "DISCONNECT"
KBYE = "KBYE"

# ADMINISTRATIVE COMMANDS
ROUTER_SHOW_WORKERS = 'ROUTER_SHOW_WORKERS'
ROUTER_SHOW_SCHEDULERS = 'ROUTER_SHOW_SCHEDULERS'

# ENVIRONMENT VARIABLES
ENV_BROKER_ADDR = 'EMQ_BROKER_ADDR'
