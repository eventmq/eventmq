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
import unittest

from .. import exceptions
from ..utils import messages


class TestCase(unittest.TestCase):
    def test_generate_msgid(self):
        msgid = messages.generate_msgid()

        self.assertEqual(type(msgid), str)

    def test_parse_message(self):
        emq_headers = ('myid', '', 'protoversion', 'command', 'msgid')
        emq_frame_singlemsg = emq_headers + ('my message',)
        emq_frame_manymsg = emq_headers + ('many', 'parts')
        emq_frame_nomsg = emq_headers

        singlemsg = messages.parse_router_message(emq_frame_singlemsg)
        manymsg = messages.parse_router_message(emq_frame_manymsg)
        nomsg = messages.parse_router_message(emq_frame_nomsg)

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
            messages.parse_router_message(broken_message)

    def test_parse_router_message(self):
        ['aef451a0-5cef-4f03-818a-221061c8ab68', '', 'eMQP/1.0', 'INFORM', '5caeb5fd-15d4-4b08-89e8-4e536672eef3', 'default', 'worker']
