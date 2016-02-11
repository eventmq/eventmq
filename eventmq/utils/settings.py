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
:mod:'settings' -- Settings Utilities """
import os
import ConfigParser
from .. import conf
import logging

logger = logging.getLogger(__name__)


def import_settings():
    """
    Import settings and apply to configuration globals
    """

    config = ConfigParser.ConfigParser()

    if os.path.exists(conf.CONFIG_FILE):
        config.read(conf.CONFIG_FILE)
        for name, value in config.items('settings'):
            if hasattr(conf, name.upper()):
                setattr(conf, name.upper(), value)
                logger.debug("Setting conf.%s to %s" % (name, value))
            else:
                logger.warning('Tried to set invalid setting: %s' % name)
