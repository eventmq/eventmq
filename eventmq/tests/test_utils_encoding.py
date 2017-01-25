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

from ..utils import encoding


class TestCase(unittest.TestCase):
    def test_encodify_list_tuple(self):
        # Test nested list
        test_list = [
            'one', 1, ['two', 2, ('three', 3)]
        ]
        self.assertEqual(encoding.encodify(test_list),
                         [b'one', 1, [b'two', 2, (b'three', 3)]])

        # Test nested tuple
        test_tuple = (
            'one', 1, ('two', 2,
                       ['three', 3],
                       {'asdf': 90, 'bp': 'adam'})
        )

        self.assertEqual(encoding.encodify(test_tuple),
                         (b'one', 1, (b'two', 2,
                                      [b'three', 3],
                                      {'asdf': 90, 'bp': b'adam'})))

    def test_encodify_dict(self):
        test_dict = {
            'key1': 1,
            'key2': 'two',
            'key_list': [{'burp': 'derp'}, ('fjdk', 3)]
        }

        self.assertEqual(encoding.encodify(test_dict),
                         {'key_list': [
                             {'burp': b'derp'},
                             (b'fjdk', 3)],
                          'key1': 1,
                          'key2': b'two'})

    def test_encodify_str(self):
        self.assertEqual(encoding.encodify('asdf'), b'asdf')

    def test_encodify_int(self):
        self.assertEqual(encoding.encodify(1), 1)

    def test_encodify_float(self):
        self.assertEqual(encoding.encodify(1.0), 1.0)
