__author__ = 'EventMQ Contributors'
__version__ = '0.3-rc10'

PROTOCOL_VERSION = 'eMQP/1.0'

from .client.messages import defer_job  # noqa
from .client.jobs import job  # noqa
