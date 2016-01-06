#!/usr/bin/python
#
# Copyright 2015 Pinterest, Inc
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

from kingpin.manageddata.managed_datastructures import ManagedList
from kingpin.manageddata.managed_datastructures import ManagedHashMap
import mock_zk_config_manager

TEST_ZK_HOSTS = ['observerzookeeper010:2181']
TEST_AWS_KEYFILE = "test_keyfile"
TEST_S3_BUCKET = "test_bucket"


class ConfigBasedManagedListsTestCase(TestCase):

    def setUp(self):
        self.mock_zk_config_manager = mock.patch(
            'kingpin.manageddata.managed_datastructures.ZKConfigManager', mock_zk_config_manager.MockZkConfigManager)
        self.mock_zk_config_manager.start()

        self.managed_list = ManagedList(
            'test_config_domain', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)

    def tearDown(self):
        self.mock_zk_config_manager.stop()

    def test_basic_operations(self):
        """Check basic add, remove, count and etc. operations for config based managed list."""
        self.assertEqual([], self.managed_list.get_list())

        self.assertEqual(1, self.managed_list.add('a.com'))
        self.assertEqual(['a.com'], self.managed_list.get_list())
        self.assertEqual(set(['a.com']), self.managed_list.get_set())
        self.assertTrue(self.managed_list.contains('a.com'))
        self.assertFalse(self.managed_list.contains('b.com'))
        self.assertEqual(1, self.managed_list.count())
        self.assertEqual(0, self.managed_list.add('a.com'))

        self.assertEqual(1, self.managed_list.add('b.com'))
        self.assertEqual(['a.com', 'b.com'], self.managed_list.get_list())
        self.assertEqual(set(['a.com', 'b.com']), self.managed_list.get_set())
        self.assertTrue(self.managed_list.contains('a.com'))
        self.assertTrue(self.managed_list.contains('b.com'))
        self.assertEqual(2, self.managed_list.count())

        self.assertEqual(0, self.managed_list.remove('c.com'))
        self.assertEqual(1, self.managed_list.remove('a.com'))
        self.assertEqual(['b.com'], self.managed_list.get_list())
        self.assertEqual(set(['b.com']), self.managed_list.get_set())
        self.assertFalse(self.managed_list.contains('a.com'))
        self.assertTrue(self.managed_list.contains('b.com'))
        self.assertEqual(1, self.managed_list.count())

        self.managed_list.delete()
        self.assertEqual([], self.managed_list.get_list())
        self.assertEqual(0, self.managed_list.count())

        self.assertEqual(2, self.managed_list.add_many(['test1', 'test2']))
        self.assertEqual(self.managed_list.get_set(), set(['test1', 'test2']))

        self.assertEqual(2, self.managed_list.add_many(
            ['test1', 'test4', 'test5']))
        self.assertEqual(self.managed_list.get_set(), set(
            ['test1', 'test2', 'test4', 'test5']))

        self.assertEqual(1, self.managed_list.set_list(["abc"]))
        self.assertEqual(["abc"], self.managed_list.get_list())

    def test_type_enforcement(self):
        ml = ManagedList(
            'test_config_domain', 'test_key1', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET, data_type=int)
        self.assertTrue(ml.data_type is int)
        ml.clear()
        self.assertEqual([], ml.get_list())
        self.assertEqual(2, ml.add_many([123, 456]))
        # reject operations with invalid values
        self.assertRaises(ValueError, ml.add, "xyz")
        self.assertRaises(ValueError, ml.add_many, [789, "101", "78a"])

        # the previous two invalid operation should not change the list, even some of the values are valid
        self.assertEqual([123, 456], ml.get_list())

        # accept `convertable' values
        self.assertEqual(0, ml.add("123"))  # valid but already exists
        self.assertEqual(1, ml.add("789"))
        self.assertEqual([123, 456, 789], ml.get_list())
        self.assertEqual(1, ml.add_many([789, "101"]))
        self.assertEqual([123, 456, 789, 101], ml.get_list())

        # reject set_list if the new list contains invalid value
        self.assertRaises(ValueError, ml.set_list, [123, "xxx"])
        self.assertEqual([123, 456, 789, 101], ml.get_list())  # set_list should be atomic
        self.assertRaises(ValueError, ml.remove, "xyz")
        self.assertEqual(1, ml.remove(123))
        self.assertEqual(1, ml.remove("456"))
        self.assertEqual([789, 101], ml.get_list())

        ml.clear()
        self.assertEqual([], ml.get_list())

        ml = ManagedList(
            'test_config_domain', 'test_key2', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET,
            data_type=str)
        self.assertTrue(ml.data_type is str)
        ml.clear()

        # test str type
        self.assertEqual([], ml.get_list())
        self.assertEqual(2, ml.add_many([123, "456"]))
        self.assertEqual(1, ml.add_many(["123", 789]))
        self.assertEqual(["123", "456", "789"], ml.get_list())
        ml.set_list([123, 456, 789])
        self.assertEqual(["123", "456", "789"], ml.get_list())  # values should be converted to specified type

    def test_corrupted_config_data(self):
        """Check managed list only updated to the new config data if it is the correct format,
        otherwise default to empty list."""
        self.managed_list.zk_config_manager._data = "[dasdsad"
        self.managed_list._reload_config_data()
        self.assertEqual([], self.managed_list.get_list())

        self.managed_list.zk_config_manager._data = "{}"
        self.managed_list._reload_config_data()
        self.assertEqual([], self.managed_list.get_list())


class ManagedDataStructureSingletonMetaclassTestCase(TestCase):

    def test_singleton_metaclass(self):
        """Check ManagedDataStructure would return singleton instance for unique identifier of
        (list_domain, list_key)."""
        test_md = ManagedList(list_domain="test_singleton_domain", list_key="test_key1",
                              list_name="", list_description="",
                              zk_hosts=TEST_ZK_HOSTS, aws_keyfile=TEST_AWS_KEYFILE, s3_bucket=TEST_S3_BUCKET)
        test_md2 = ManagedList(list_domain="test_singleton_domain", list_key="test_key1",
                               list_name="", list_description="",
                               zk_hosts=TEST_ZK_HOSTS, aws_keyfile=TEST_AWS_KEYFILE, s3_bucket=TEST_S3_BUCKET)
        test_md3 = ManagedList(list_domain="test_singleton_domain", list_key="test_key2",
                               list_name="", list_description="",
                               zk_hosts=TEST_ZK_HOSTS, aws_keyfile=TEST_AWS_KEYFILE, s3_bucket=TEST_S3_BUCKET)
        test_md4 = ManagedList(list_domain="test_singleton_domain2", list_key="test_key1",
                               list_name="", list_description="",
                               zk_hosts=TEST_ZK_HOSTS, aws_keyfile=TEST_AWS_KEYFILE, s3_bucket=TEST_S3_BUCKET)
        self.assertEqual(test_md, test_md2)
        self.assertFalse(test_md == test_md3)
        self.assertFalse(test_md == test_md4)

        # Instance corresponding to the unique identifier of (list_domain, list_key) can only be
        # one type.
        with self.assertRaises(Exception):
            ManagedHashMap(list_domain="test_singleton_domain", list_key="test_key1",
                           list_name="", list_description="",
                           zk_hosts=TEST_ZK_HOSTS, aws_keyfile=TEST_AWS_KEYFILE, s3_bucket=TEST_S3_BUCKET)
