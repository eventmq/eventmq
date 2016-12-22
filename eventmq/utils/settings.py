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
from configparser import ConfigParser
import json
import logging
import os

from . import tuplify
from .. import conf


logger = logging.getLogger(__name__)


def import_settings(section='global'):
    """
    Import settings and apply to configuration globals

    Args:
       section (str): Name of the INI section to import
    """
    config = ConfigParser()

    if os.path.exists(conf.CONFIG_FILE):
        config.read(conf.CONFIG_FILE)

        if not config.has_section(section):
            logger.warning(
                'Tried to read nonexistent section {}'.format(section))
            return

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
                logger.warning('Tried to set invalid setting: %s' % name)
    else:
        logger.warning('Config file at {} not found. Continuing with '
                       'defaults.'.format(conf.CONFIG_FILE))
