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
:mod:`settings` -- Settings Utilities
=====================================
Settings are defined in the :data:`_CONFIG_DEFS` dictionary. They are organized
by section. This dictionary is used to build the command line options with the
helper scripts, and validate the options loaded from the command line and
config files.

To add a configuration option add an all caps name that describes it's
value. This is what you will reference when accessing the option from the
``conf`` object. The value of this key is another dictionary with the following
options:

- ``default``: This is the default value for the option.
- ``long-arg``: This is the long version of the command line argument. It is
  required if you want to enable the option on the command line.
- ``short-arg``: This is the short version of the command line
  argument. Optional if you don't need one.
- ``help``: Help text to be displayed when --help is passed
- ``nargs``: Number of arguments.
  - N (integer; N arguments gathered as a list)
  - ? (one argument gathered as a string)
  - * (all arguments gathered as a list)
  - + (all arguments gathered as a list. at least 1 argument is required.)

For more information see:
https://docs.python.org/2/library/argparse.html#the-add-argument-method
"""
import argparse
from configparser import ConfigParser
from copy import copy
import json
import logging
import os

# for py2 since dict().items() is inefficient
from future.utils import iteritems

from . import constants
from .utils import tuplify


logger = logging.getLogger(__name__)


#: Define aliases for command line options to their ``conf`` constant name.
#: Sometimes you want the long arg to be different than what the constant name
#: translates to. In that case use this dict to alias the argparse option name
#: to the conf constatn.
_CLI_ARG_ALIASES = {
    # Command line option:  Constant name
    'CONFIG': 'CONFIG_FILE',
    'HIGH_WATER_MARK': 'HWM',
}

_CONFIG_DEFS = {
    # Options defined in the global section are also valid if they are found in
    # any other section.
    'global': {
        'SUPER_DEBUG': {
            'default': False,
            'long-arg': '--super-debug',
            'short-arg': None,
            'type': bool,
            'help': 'Output the messages that are passed to and from the '
                    'device into the debug log.',
        },
        'HIDE_HEARTBEAT_LOGS': {
            'default': True,
            'long-arg': '--hide-heartbeat-logs',
            'short-arg': None,
            'type': bool,
            'help': 'When SUPER_DEBUG is also enabled, hide the HEARTBEAT '
                    'messages. Useful only for debugging connection issues.',
        },
        'DEFAULT_QUEUE_NAME': {
            'default': 'default',
            'long-arg': '--default-queue-name',
            'short-arg': None,
            'type': str,
            'help': 'When no queue name is specified by a job use this queue.',
        },
        'DISABLE_HEARTBEATS': {
            'default': False,
            'long-arg': '--disable-heartbeats',
            'short-arg': None,
            'type': bool,
            'help': 'Disable heartbeating.'
        },
        'HEARTBEAT_LIVENESS': {
            'default': 3,
            'long-arg': '--heartbeat-liveness',
            'short-arg': None,
            'type': int,
            'help': 'Assume the peer is dead after this many missed heartbeats'
        },
        'HEARTBEAT_TIMEOUT': {
            'default': 5,
            'long-arg': '--heartbeat-timeout',
            'type': int,
            'help': 'Count a heartbeat as missed after this many seconds',
        },
        'HEARTBEAT_INTERVAL': {
            'default': 3,
            'long-arg': '--heartbeat-interval',
            'type': int,
            'help': 'Number of seconds to wait between sending each heartbeat',
        },
        'CONFIG_FILE': {
            'default': '/etc/eventmq.conf',
            'long-arg': '--config',
            'short-arg': '-C',
            'type': str,
            'help': 'manually specify location of eventmq.conf',
        },
        'NAME': {
            'default': None,
            'long-arg': '--name',
            'short-arg': '-n',
            'type': str,
            'help': "A unique ame to give this node. If one isn't provided a "
                    "random uuid will be generated",
        },
    },
    'router': {
        'FRONTEND_LISTEN_ADDR': {
            'default': 'tcp://127.0.0.1:47291',
            'long-arg': '--frontend-listen-addr',
            'short-arg': '-F',
            'type': str,
            'help': 'Address to listen for clients and schedulers',
        },
        'BACKEND_LISTEN_ADDR': {
            'default': 'tcp://127.0.0.1:47290',
            'long-arg': '--backend-listen-addr',
            'short-arg': '-B',
            'type': str,
            'help': 'Address to listen for job managers',
        },
        'ADMINISTRATIVE_LISTEN_ADDR': {
            'default': 'tcp://127.0.0.1:47293',
            'long-arg': '--administrative-listen-addr',
            'short-arg': '-D',
            'type': str,
            'help': 'Address to listen for administrative commands',
        },
        'HWM': {
            'default': 10000,
            'long-arg': '--high-water-mark',
            'type': str,
            'help': 'Number of messages to store in memory before dropping '
                    'them.'
        },
    },
    'jobmanager': {
        'CONNECT_ADDR': {
            'default': 'tcp://127.0.0.1:47290',
            'long-arg': '--connect-addr',
            'short-arg': '-A',
            'type': str,
            'help': "Address of the router's BACKEND_LISTEN_ADDR.",
            'env': constants.ENV_BROKER_ADDR
        },
        'QUEUES': {
            'default': [[10, "default"]],
            'long-arg': '--queues',
            'short-arg': '-Q',
            'type': list,
            'nargs': '+',
            'help': "Space seperated list of queues. Seperate the queue "
                    "weight from the name with a comma. For example "
                    "-Q 10,default 20,high"
        },
        'MAX_JOB_COUNT': {
            'default': 1024,
            'long-arg': '--max-job-count',
            'type': int,
            'help': "Maximum number of jobs each worker process executes "
                    "before resetting",
        },
        'CONCURRENT_JOBS': {
            'default': 4,
            'long-arg': '--concurrent-jobs',
            'short-arg': '-J',
            'type': int,
            'help': 'Number of concurrent jobs to execute.'
        },
        'SETUP_PATH': {
            'default': '',
            'long-arg': '--setup-path',
            'type': str,
            'help': 'Module path to SETUP_CALLABLE'
        },
        'SETUP_CALLABLE': {
            'default': '',
            'long-arg': '--setup-callable',
            'type': str,
            'help': 'Callable name found at SETUP_PATH',
        },
        'KILL_GRACE_PERIOD': {
            'default': 300,
            'long-arg': '--kill-grace-period',
            'type': int,
            'help': 'Seconds to wait before forefully killing worker '
                    'processes after receiving a SIGTERM',
        },
        'GLOBAL_TIMEOUT': {
            'default': 300,
            'long-arg': '--global-timeout',
            'short-arg': '-t',
            'type': int,
            'help': "Global default timeout for all jobs unless the request "
                    "specifies otherwise",
        }
    },
    'scheduler': {
        'CONNECT_ADDR': {
            'default': 'tcp://127.0.0.1:47291',
            'long-arg': '--connect-addr',
            'short-arg': '-A',
            'type': str,
            'help': "Address of the router's FRONTEND_LISTEN_ADDR.",
            'env': constants.ENV_BROKER_ADDR,
        },
        'QUEUES': {
            'default': [[10, "default"]],
            'long-arg': '--queues',
            'short-arg': '-Q',
            'type': list,
            'nargs': '+',
            'help': "Space seperated list of queues. Seperate the queue "
                    "weight from the name with a comma. For example "
                    "-Q 10,default 20,high"
        },
        'REDIS_HOST': {
            'default': 'localhost',
            'long-arg': '--redis-host',
            'type': str,
            'help': 'Host name of the redis server to use for persisting '
                    'scheduled jobs.'
        },
        'REDIS_PORT': {
            'default': 6379,
            'long-arg': '--redis-port',
            'type': int,
            'help': 'Port of the redis server.',
        },
        'REDIS_DB': {
            'default': 0,
            'long-arg': '--redis-db',
            'type': int,
            'help': 'Redis database to use',
        },
        'REDIS_PASSWORD': {
            'default': '',
            'long-arg': '--redis-password',
            'type': str,
            'help': 'Password to redis'
        },
        'ADMINISTRATIVE_LISTEN_ADDR': {
            'default': 'tcp://127.0.0.1:47294',
            'long-arg': '--administrative-listen-addr',
            'short-arg': '-S',
            'type': str,
            'help': 'Address to listen for administrative commands for '
                    'schedulers',
        },
    },
    'publisher': {
        'FRONTEND_LISTEN_ADDR': {
            'default': 'tcp://127.0.0.1:47298',
            'long-arg': '--frontend-listen-addr',
            'short-arg': '-F',
            'type': str,
            'help': 'Address to listen for client PUBLISH messages',
        },
        'BACKEND_LISTEN_ADDR': {
            'default': 'tcp://127.0.0.1:47299',
            'long-arg': '--backend-listen-addr',
            'short-arg': '-B',
            'type': str,
            'help': 'Address to listen for subscribers on',
        },
    },
}


def reload_settings(section, override_settings=None):
    """
    Reload settings by resetting ``conf`` to defaults, loading settings from a
    file and overwriting any additional settings with what's defined in the
    ``override_settings`` dict

    Args:
        section (str): The section of interest. See ``_CONFIG_DEFS``
        override_settings (dict): Dictionary containing any additional settings
           to override. The key should be the upper case config name.
           See: :func:`load_settings_from_dict` for more information.
    """
    if section not in _CONFIG_DEFS:
        raise ValueError(
            'Unable to reload settings using unknown section: {} - Valid '
            'options are: {}'.format(section, _CONFIG_DEFS.keys()))

    config_file = None

    conf.reload()

    if override_settings:
        if 'CONFIG' in override_settings:
            config_file = override_settings['CONFIG']
        elif 'CONFIG_FILE' in override_settings:
            config_file = override_settings['CONFIG_FILE']

    load_settings_from_file(section, file_path=config_file)
    load_settings_from_dict(override_settings, section)

    conf.section = section


def load_settings_from_dict(settings_dict, section):
    """
    Load settings into conf from the provided dictionary. Generally this would
    be called after :func:`load_settings_from_file` to override any settings
    that may be loaded there. In practice this is used for the command line
    options.

    The dictonary's key is the uppercase config name, and it's value the config
    value. For example:

    .. code:: python

       {
           'SUPER_DEBUG': True,
           'DISABLE_HEARTBEATS': True,
       }

    Args:
        settings_dict (dict): Dictionary containing the settings you wish to
            load.
        section (str): The name of the config section these settings should be
            validated against.
    """
    if section not in _CONFIG_DEFS:
        raise ValueError('Unable to load_settings_from_dict using unknown '
                         'section: {} - Valid options are: {}'.format(
                             section, _CONFIG_DEFS.keys()))

    # Define the conf section based on what we're about to load
    conf.section = section

    if settings_dict is None:
        return

    for k, v in iteritems(settings_dict):
        # Use the CLI argument alias if that exists
        if k in _CLI_ARG_ALIASES:
            k = _CLI_ARG_ALIASES[k]

        if k in _CONFIG_DEFS['global'] or \
           _CONFIG_DEFS[section]:
            logger.debug("Setting conf.{} to {}".format(k, v))
            setattr(conf, k, v)
        else:
            logger.warning('Unknown setting {}={} when loading settings from '
                           'dict (section:{}). Skipping...'.format(
                               k, v, section))


def load_settings_from_file(section='global', file_path=None):
    """
    Import settings from the defined config file. This will import all the
    settings from the ``global`` section followed by the requested section. The
    default location is used unless it has been overridden via ``file_path``
    (or set on ``conf`` elsewhere.)

    Args:
       section (str): Name of the INI section to import
       file_path (str): Full filesystem path of the config file to load.
    """
    if section not in _CONFIG_DEFS:
        raise ValueError('Unable to load_settings_from_file using unknown '
                         'section: {} - Valid options are: {}'.format(
                             section, _CONFIG_DEFS.keys()))

    # Define the conf section based on what we're about to load
    conf.section = section

    config = ConfigParser()

    if file_path:
        conf.CONFIG_FILE = file_path

    if not os.path.exists(conf.CONFIG_FILE):
        logger.warning('Config file at {} not found. Continuing with '
                       'defaults.'.format(conf.CONFIG_FILE))

    config.read(conf.CONFIG_FILE)

    if config.has_section('global'):
        _load_section(config, 'global')

    if section == 'global':
        # If the requested section is the default then there is nothing
        # left to do
        return

    if not config.has_section(section):
        logger.warning(
            'Tried to read nonexistent section {}'.format(section))
        return

    _load_section(config, section)


def _load_section(config, section):
    """
    Load the requested section into the configuration globals in
    :mod:`eventmq.conf`

    Args:
        section (str): Name of the INI section to import
    """
    for name, value in config.items(section):
        if hasattr(conf, name.upper()):

            default_value = getattr(conf, name.upper())
            t = type(default_value)
            if isinstance(default_value, (list, tuple)):
                try:
                    value = t(json.loads(value))
                except ValueError:
                    raise ValueError(
                        "Invalid JSON syntax for {} setting".format(name))
                # json.loads coverts all arrays to lists, but if the first
                # element in the default is a tuple (like in QUEUES) then
                # convert those elements, otherwise whatever it's type is
                # correct
                if isinstance(default_value[0], tuple):
                    setattr(conf, name.upper(),
                            t(map(tuplify, value)))
                else:
                    setattr(conf, name.upper(), t(value))
            elif isinstance(default_value, bool):
                setattr(conf, name.upper(),
                        True if 't' in value.lower() else False)
            else:
                setattr(conf, name.upper(), t(value))
            logger.debug("Setting conf.{} to {}".format(
                name.upper(), getattr(conf, name.upper())))
        else:
            logger.warning('Tried to set invalid setting: {}={}'.format(
                name, value))


def build_parser_arguments(parser, section):
    """
    Add arguments to an :class:`argparse.ArgumentParser` that are valid for the
    given ``section``.

    Args:
        parser (:class:`argparse.ArgumentParser`): the argparser for the
            application.
        section (str): the section of the config to generate arguments for.
    """
    for s in ('global', section):
        for k, v in iteritems(_CONFIG_DEFS[s]):
            if not v.get('long-arg'):
                continue

            add_argument_args = (v.get('long-arg'),)
            if v.get('short-arg'):
                add_argument_args += (v.get('short-arg'),)

            possible_kwargs = ('type', 'help', 'nargs', 'action')
            add_argument_kwargs = {'default': argparse.SUPPRESS}
            for kwarg in possible_kwargs:
                if v.get(kwarg):
                    if kwarg == 'type' and v.get(kwarg) == bool:
                        # Note: The action for bools are the reverse of their
                        # default.
                        if v.get('default'):
                            add_argument_kwargs['action'] = 'store_false'
                        else:
                            add_argument_kwargs['action'] = 'store_true'
                    else:
                        add_argument_kwargs[kwarg] = v.get(kwarg)

            parser.add_argument(*add_argument_args, **add_argument_kwargs)


class Conf(object):
    """
    Used as a singleton to expose configuration values to the rest of the app.

    Define ``conf.section`` to the config section this config is for in order
    to read the default values from that section. If section is ``None`` then
    only the global section will be scanned and you may end up with an
    ``AttributeError`` For example:

    .. code:: python

       conf.reload()
       conf.section = 'jobmanager'

       print conf.QUEUES
    """
    _instance = None

    class _impl(object):
        # The class/object that actually holds the configuration values

        #: No real reason to allow changes to this
        DEFAULT_ENCODING = 'utf-8'

        def __init__(self, section=None):
            self.section = section

    def __init__(self, section=None):
        if not Conf._instance:
            self.reload()
        setattr(Conf._instance, 'section', section)

    def reload(self):
        """
        Delete and recreate the config instance object. This essentially resets
        the config to it's default values.
        """
        section = None
        if hasattr(Conf._instance, 'section'):
            section = copy(Conf._instance.section)

        del Conf._instance
        Conf._instance = Conf._impl(section)

    def __getattr__(self, name):
        if hasattr(self._instance, name):
            return getattr(self._instance, name)

        # Check for defaults if it isn't already defined and return the default
        # if it exists raising an AttributeError if it doesn't.
        if name in _CONFIG_DEFS['global']:
            setting = _CONFIG_DEFS['global'][name]
        elif self._instance.section and \
             self._instance.section in _CONFIG_DEFS and \
             name in _CONFIG_DEFS[self._instance.section]:  # noqa
            setting = _CONFIG_DEFS[self._instance.section][name]
        else:
            raise AttributeError('{} does not exist'.format(name))

        try:
            value = setting['default']
        except KeyError:
            value = None

        # Cache the value for later
        setattr(self._instance, name, value)

        return value

    def __setattr__(self, name, value):
        return setattr(self._instance, name, value)


conf = Conf()
