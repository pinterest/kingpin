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

from kingpin.manageddata.managed_datastructures import ManagedMappedList
import mock_zk_config_manager

TEST_ZK_HOSTS = ['observerzookeeper010:2181']
TEST_AWS_KEYFILE = "test_keyfile"
TEST_S3_BUCKET = "test_bucket"


class ManagedMappedListTestCase(TestCase):

    def setUp(self):
        self.mock_zk_config_manager = mock.patch(
            'kingpin.manageddata.managed_datastructures.ZKConfigManager', mock_zk_config_manager.MockZkConfigManager)
        self.mock_zk_config_manager.start()

    def tearDown(self):
            self.mock_zk_config_manager.stop()

    def test_get(self):
        managed_mapped_list = ManagedMappedList(
            'test_get', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        self.assertEqual({}, managed_mapped_list.get_all())
        self.assertEqual([], managed_mapped_list.get_list('key'))

    def test_empty_remove(self):
        managed_mapped_list = ManagedMappedList(
            'test_empty_remove', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        self.assertTrue(managed_mapped_list.remove_list('key'))
        self.assertTrue(managed_mapped_list.remove_item('key', 'val'))
        self.assertTrue(managed_mapped_list.remove_items('key', ['val']))

    def test_set_dedup(self):
        managed_mapped_list = ManagedMappedList(
            'test_set_unique', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        self.assertTrue(managed_mapped_list.set_list('key', [1, 2]))
        self.assertEqual([1, 2], managed_mapped_list.get_list('key'))

    def test_set(self):
        managed_mapped_list = ManagedMappedList(
            'test_set', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET,
            dedup=False)
        self.assertTrue(managed_mapped_list.set_list('key', [1, 2, 2]))
        self.assertEqual([1, 2, 2], managed_mapped_list.get_list('key'))

    def test_add_item_dedup(self):
        managed_mapped_list = ManagedMappedList(
            'test_add_item_dedup', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        self.assertTrue(managed_mapped_list.add_item('key', 2))
        self.assertEqual([2], managed_mapped_list.get_list('key'))

        self.assertTrue(managed_mapped_list.add_item('key', 2))
        self.assertEqual([2], managed_mapped_list.get_list('key'))

        self.assertTrue(managed_mapped_list.add_item('key', 3))
        self.assertEqual([2, 3], managed_mapped_list.get_list('key'))

    def test_add_item(self):
        managed_mapped_list = ManagedMappedList(
            'test_add_item', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET,
            dedup=False)
        self.assertTrue(managed_mapped_list.add_item('key', 2))
        self.assertEqual([2], managed_mapped_list.get_list('key'))
        self.assertTrue(managed_mapped_list.add_item('key', 2))
        self.assertEqual([2, 2], managed_mapped_list.get_list('key'))

    def test_add_items_dedup(self):
        managed_mapped_list = ManagedMappedList(
            'test_add_items_dedup', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        self.assertTrue(managed_mapped_list.add_items('key', [2, 2, 3]))
        self.assertEqual([2, 3], managed_mapped_list.get_list('key'))
        self.assertTrue(managed_mapped_list.add_items('key', [3]))
        self.assertEqual([2, 3], managed_mapped_list.get_list('key'))

    def test_add_items(self):
        managed_mapped_list = ManagedMappedList(
            'test_add_items', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET,
            dedup=False)
        self.assertTrue(managed_mapped_list.add_items('key', [2, 2, 3]))
        self.assertEqual([2, 2, 3], managed_mapped_list.get_list('key'))
        self.assertTrue(managed_mapped_list.add_items('key', [2, 3]))
        self.assertEqual([2, 2, 3, 2, 3], managed_mapped_list.get_list('key'))

    def test_set_and_remove_dedup(self):
        managed_mapped_list = ManagedMappedList(
            'test_set_and_remove_dedup', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        managed_mapped_list.set_list('key', [1, 2, 2, 3, 2])
        self.assertTrue(managed_mapped_list.remove_list('key'))
        self.assertEqual([], managed_mapped_list.get_list('key'))

        managed_mapped_list.set_list('key', [1, 2, 2, 3, 2])
        self.assertTrue(managed_mapped_list.remove_item('key', 2))
        self.assertEqual([1, 3], managed_mapped_list.get_list('key'))

        managed_mapped_list.set_list('key', [1, 2, 2, 3, 2])
        self.assertTrue(managed_mapped_list.remove_items('key', [2, 3]))
        self.assertEqual([1], managed_mapped_list.get_list('key'))

    def test_set_and_remove(self):
        managed_mapped_list = ManagedMappedList(
            'test_set_and_remove', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET,
            dedup=False)
        managed_mapped_list.set_list('key', [1, 2, 2, 3, 2])
        self.assertTrue(managed_mapped_list.remove_list('key'))
        self.assertEqual([], managed_mapped_list.get_list('key'))

        managed_mapped_list.set_list('key', [1, 2, 2, 3, 2])
        self.assertTrue(managed_mapped_list.remove_item('key', 2))
        self.assertEqual([1, 2, 3, 2], managed_mapped_list.get_list('key'))

        managed_mapped_list.set_list('key', [1, 2, 2, 3, 2])
        self.assertTrue(managed_mapped_list.remove_items('key', [2, 3]))
        self.assertEqual([1, 2, 2], managed_mapped_list.get_list('key'))

    def test_contains(self):
        managed_mapped_list = ManagedMappedList(
            'test_contains', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        managed_mapped_list.set_list('key', [1, 2, 2, 3, 2])
        self.assertTrue(managed_mapped_list.contain_item('key', 2))
        self.assertFalse(managed_mapped_list.contain_item('key2', 2))
        self.assertFalse(managed_mapped_list.contain_item('key', 4))

        self.assertEqual([2], managed_mapped_list.contain_items('key', [2, 4]))
        self.assertEqual([], managed_mapped_list.contain_items('key2', [2, 4]))
        self.assertEqual([], managed_mapped_list.contain_items('key', [4]))

    def _update(self):
        pass

    def test_callback(self):
        update_func = mock.Mock()
        _patch = mock.patch.object(
            self, '_update', update_func)
        _patch.start()
        self.addCleanup(_patch.stop)

        managed_mapped_list = ManagedMappedList(
            'test_domain5', 'test_key', 'test_name', 'test_description',
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET,
            update_callback=self._update)
        self.assertTrue(managed_mapped_list.zk_config_manager.update_zk('{}', '{1:[2]}'))
        self.assertTrue(update_func.is_called)
