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
import unittest

from .. import exceptions
from .. import utils
import utils.messages


class TestCase(unittest.TestCase):
    def test_generate_msgid(self):
        msgid = utils.messages.generate_msgid()

        self.assertEqual(type(msgid), str)

    def test_parse_message(self):
        emq_headers = ('myid', '', 'protoversion', 'command', 'msgid')
        emq_frame_singlemsg = emq_headers + ('my message',)
        emq_frame_manymsg = emq_headers + ('many', 'parts')
        emq_frame_nomsg = emq_headers

        singlemsg = utils.messages.parse_router_message(emq_frame_singlemsg)
        manymsg = utils.messages.parse_router_message(emq_frame_manymsg)
        nomsg = utils.messages.parse_router_message(emq_frame_nomsg)

        self.assertEqual(singlemsg[0], emq_frame_singlemsg[0])
        self.assertEqual(singlemsg[1], emq_frame_singlemsg[3])
        self.assertEqual(singlemsg[2], emq_frame_singlemsg[4])
        self.assertEqual(singlemsg[3], (emq_frame_singlemsg[5],))

        self.assertEqual(manymsg[0], emq_frame_manymsg[0])
        self.assertEqual(manymsg[1], emq_frame_manymsg[3])
        self.assertEqual(manymsg[2], emq_frame_manymsg[4])
        self.assertEqual(manymsg[3], emq_frame_manymsg[5:])

        self.assertEqual(nomsg[0], emq_frame_nomsg[0])
        self.assertEqual(nomsg[1], emq_frame_nomsg[3])
        self.assertEqual(nomsg[2], emq_frame_nomsg[4])
        self.assertEqual(nomsg[3], ())

        broken_message = ('dlkajfs', 'lkasdjf')
        with self.assertRaises(exceptions.InvalidMessageError):
            utils.messages.parse_router_message(broken_message)
