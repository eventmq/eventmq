# This file is part of eventmq.
#
# eventmq is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# eventmq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with eventmq.  If not, see <http://www.gnu.org/licenses/>.
"""
:mod:`logster` - Logster Parser
================================
Logster is a utility for parsing log files and outputting info the graphing
app graphite understands. This file contains the logster parser for eventmq.

Using this at the very minimum requires the logster package, but you should
also set up something for logster to report this data to.

See: https://github.com/etsy/logster
"""
import re
from logster.logster_helper import LogsterParser, MetricObject


class EventMQParser(LogsterParser):
    def __init__(self):
        #: Count of messages process by the router
        self.processed_messages = 0L
        #: Number of waiting jobs on the router
        self.waiting_messages = 0L
        #: Number of jobs scheduled with the scheduler
        self.jobs_scheduled = 0L  # TODO:

        self.processed_message_re = re.compile(
            r'.*Received message.*REQUEST.*')
        self.waiting_message_re = re.compile(
            r'.*No available workers for queue "(.*)".*')

    def parse_line(self, line):
        processed_message_match = self.processed_message_re.match(line)

        if processed_message_match:
            self.processed_messages += 1

        waiting_messages_match = self.waiting_message_re.match(line)

        if waiting_messages_match:
            # queue_name = waiting_messages_match.groups[0]

            self.waiting_messages += 1

    def get_state(self, duration):
        duration = float(duration)
        return (
            MetricObject('Processed Messages',
                         self.processed_messages / duration),
            MetricObject('Waiting Messages',
                         self.waiting_messages / duration)
        )
