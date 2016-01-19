#!/usr/bin/python
#
# Copyright 2016 Pinterest, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mock
from unittest import TestCase

from kingpin.manageddata.managed_datastructures import ManagedHashMap
import mock_zk_config_manager

TEST_ZK_HOSTS = ['observerzookeeper010:2181']
TEST_AWS_KEYFILE = "test_keyfile"
TEST_S3_BUCKET = "test_bucket"


class ConfigBasedManagedHashMapTestCase(TestCase):
    def setUp(self):
        self.mock_zk_config_manager = mock.patch(
            "kingpin.manageddata.managed_datastructures.ZKConfigManager",
            mock_zk_config_manager.MockZkConfigManager)
        self.mock_zk_config_manager.start()

        self.managed_map = ManagedHashMap(
            'test_domain', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)

    def tearDown(self):
        self.mock_zk_config_manager.stop()

    # Test map operations: set, set_many, get, remove etc
    def test_map_operations(self):
        self.assertEqual([], self.managed_map.get_keys())
        self.assertEqual(1, self.managed_map.set('key_foo', 'val_bar'))
        self.assertEqual('val_bar', self.managed_map.get('key_foo'))
        self.assertEqual(None, self.managed_map.get('key_foo2'))
        many_items = {'key_foo2': 'val_bar2', 'key_foo3': 'val_bar3'}
        self.assertEqual(2, self.managed_map.set_many(many_items))
        self.assertEqual('val_bar2', self.managed_map.get('key_foo2'))
        self.assertEqual(0, self.managed_map.set_many(many_items))
        many_items['key_foo3'] = 'val_bar3_3'
        self.assertEqual(1, self.managed_map.set_many(many_items))
        get_many = self.managed_map.get_many(['key_foo2', 'key_foo3'])
        self.assertEqual(2, len(get_many))
        self.assertEqual('val_bar2', get_many['key_foo2'])
        self.assertEqual('val_bar3_3', get_many['key_foo3'])
        self.assertEqual(True, self.managed_map.contains('key_foo3'))
        self.assertEqual(False, self.managed_map.contains('key_foo5'))
        self.assertEqual(True, isinstance(self.managed_map.get_all(), dict))
        self.assertEqual(3, len(self.managed_map.get_all()))
        self.assertEqual(0, self.managed_map.remove('unknown_key'))
        self.assertEqual(1, self.managed_map.remove('key_foo'))
        self.assertEqual(2, len(self.managed_map.get_keys()))
        self.managed_map.delete()
        self.assertEqual([], self.managed_map.get_keys())

        # Check set_map().
        many_items2 = {'key_foo12': 'val_bar12', 'key_foo13': 'val_bar13'}
        self.assertEqual(2, self.managed_map.set_map(many_items2))
        self.assertEqual(many_items2, self.managed_map.get_all())

    # test map operations when having key/value type specified
    def test_type_enforcement(self):
        mm = ManagedHashMap(
            'test_domain', 'test_key1', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET,
            key_type=str, value_type=int)
        self.assertTrue(mm.key_type is str)
        self.assertTrue(mm.value_type is int)
        mm.set_map({})
        self.assertEqual([], mm.get_keys())
        self.assertEqual(1, mm.set("foo", "123"))
        self.assertEqual(1, mm.set("bar", 456))
        self.assertEqual(1, mm.set(789, 789))
        self.assertEqual(set(["foo", "bar", "789"]), set(mm.get_keys()))
        self.assertEqual(123, mm.get("foo"))
        self.assertEqual(456, mm.get("bar"))
        self.assertEqual(789, mm.get("789"))

        # operations with invalid values
        self.assertRaises(ValueError, mm.set, "abc", "xyz")
        many_items = {"aaa": "111", "bbb": "bla"}
        self.assertRaises(ValueError, mm.set_many, many_items)
        self.assertRaises(ValueError, mm.set_map, many_items)
        self.assertEqual(set(["foo", "bar", "789"]), set(mm.get_keys()))  # invalid operations do not change data

        many_items = {"aaa": "111", "bbb": 222}  # valid new dict
        mm.set_map(many_items)
        self.assertEqual(set(["aaa", "bbb"]), set(mm.get_keys()))

        # test remove
        mm = ManagedHashMap(
            'test_domain', 'test_key2', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET,
            key_type=int, value_type=str)
        mm.set_map({111: "aaa", 222: "bbb", "333": "ccc"})
        self.assertEqual(set([111, 222, 333]), set(mm.get_keys()))
        self.assertRaises(ValueError, mm.remove, "xxx")
        self.assertEqual(set([111, 222, 333]), set(mm.get_keys()))
        self.assertEqual(1, mm.remove("111"))  # given key in string is able to convert
        self.assertEqual(set([222, 333]), set(mm.get_keys()))