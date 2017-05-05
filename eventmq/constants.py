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

DISCONNECT = "DISCONNECT"
KBYE = "KBYE"
STATUS_CMD = "STATUS"


class STATUS_COMMANDS(object):
    """
    Defines the STATUS sub commands
    """
    #: Router subommand to show connected job managbers
    show_managers = 'show_managers'
    #: Router subcommand to show connected schedulers
    show_schedulers = 'show_schedulers'


# ENVIRONMENT VARIABLES
ENV_BROKER_ADDR = 'EMQ_CONNECT_ADDR'
