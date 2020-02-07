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
"""

# ConfigParser was renamed to configparser in python 3. Do this try...except
# to maintain python 2/3 compatability
try:
    from configparser import ConfigParser, NoOptionError
except ImportError:
    from ConfigParser import ConfigParser, NoOptionError

import json
import logging
import os

from six.moves import map

from . import tuplify
from .. import conf


logger = logging.getLogger(__name__)


def import_settings(section='global'):
    """
    Import settings and apply to configuration globals. This function will
    also read from the environment variables and override any value defined
    in the file.

    Args:
       section (str): Name of the INI section to import
    """
    config = ConfigParser()

    config_path = os.environ.get('EVENTMQ_CONFIG_FILE', conf.CONFIG_FILE)
    use_config_file = False

    if os.path.exists(config_path):
        config.read(config_path)
        if config.has_section(section):
            use_config_file = True
        else:
            logger.warning(
                'Tried to read nonexistent section {} from {}'.format(
                    section, config_path))

    for name in dir(conf):
        if name.startswith('_'):
            continue

        value = None
        found_value = False
        default_value = getattr(conf, name)

        # Favor environment variables over the config file definition
        try:
            value = os.environ['EVENTMQ_{}'.format(name)]
            found_value = True
        except KeyError:
            if use_config_file:
                try:
                    value = config.get(section, name)
                    found_value = True
                except NoOptionError:
                    found_value = False

        if found_value:
            t = type(getattr(conf, name))

            if t in (list, tuple):
                try:
                    value = t(json.loads(value))
                except ValueError:
                    raise ValueError(
                        'Invalid JSON syntax for {} setting'.format(name))
                # json.loads coverts all arrays to lists, but if the first
                # element in the default is a tuple (like in QUEUES) then
                # convert those elements, otherwise whatever it's type is
                # correct
                if isinstance(default_value[0], tuple):
                    setattr(conf, name, t(map(tuplify, value)))
                else:
                    setattr(conf, name, t(value))
            elif isinstance(default_value, bool):
                setattr(conf, name,
                        True if 't' in value.lower() else False)
            elif t == dict:
                try:
                    value = json.loads(value)
                except ValueError:
                    raise ValueError(
                        'Invalid JSON syntax for {} setting'.format(name))
                setattr(conf, name, value)
            else:
                setattr(conf, name, t(value))
