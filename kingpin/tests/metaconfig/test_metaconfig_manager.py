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
import unittest
import simplejson
from kingpin.metaconfig.metaconfig_utils import MetaConfigManager
import mocks_for_metaconfig_tests
from kingpin.tests.kazoo_utils import testutil

TEST_ZK_HOSTS = ['observerzookeeper010:2181']
TEST_AWS_KEYFILE = "test_keyfile"
TEST_S3_BUCKET = "test_bucket"


class MetaConfigManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.mock_zk_config_manager = mock.patch(
            "kingpin.config_utils.ZKBaseConfigManager",
            mocks_for_metaconfig_tests.MockZkBaseConfigManager)
        self.mock_zk_config_manager.start()

    def tearDown(self):
        self.mock_zk_config_manager.stop()

    @mock.patch("kazoo.client.KazooClient.__new__",
                new=mock.Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_construct_download_command(self):
        metaconfig_manager = MetaConfigManager(
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        serverset_zk_path = "/discovery/testservice/prod"
        generated_download_command = metaconfig_manager.construct_zk_download_data_command_for_serverset(serverset_zk_path)
        expected_download_command = "zk_download_data.py -f /var/serverset/discovery.testservice.prod -p /discovery/testservice/prod -m serverset"
        self.assertEqual(expected_download_command, generated_download_command)

        config_zk_path = "/config/manageddata/spam/blacklist"
        generated_download_command = metaconfig_manager.construct_zk_download_data_command_for_config(config_zk_path)
        expected_download_command = 'zk_download_data.py -f /var/config/config.manageddata.spam.blacklist --from-s3 /data/config/manageddata/spam/blacklist --aws-key-file /etc/configs_readonly.conf -m config -p /config/manageddata/spam/blacklist'
        self.assertEqual(expected_download_command, generated_download_command)

    @mock.patch("kazoo.client.KazooClient.__new__",
                new=mock.Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_construct_metaconfig_with_single_section(self):
        metaconfig_manager = MetaConfigManager(
            TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        download_command = 'zk_download_data.py -f /var/serverset/discovery.testservice.prod -p /discovery/testservice/prod -m serverset'
        serverset_zk_path = "/discovery/testservice/prod"
        name = "discovery.testservice.prod"
        generated_metaconfig_content = metaconfig_manager.construct_metaconfig_with_single_section(
            name, serverset_zk_path, download_command, is_serverset=True
        )
        expected_metaconfig_content = simplejson.dumps(
            [
                {
                    "config_section_name": "discovery.testservice.prod",
                    "zk_path": "/discovery/testservice/prod",
                    "max_wait_in_secs": 0,
                    "command": "zk_download_data.py -f /var/serverset/discovery.testservice.prod -p /discovery/testservice/prod -m serverset",
                    "type": "serverset"
                }
            ]
        )
        self.assertEqual(expected_metaconfig_content, generated_metaconfig_content)

        download_command = 'zk_download_data.py -f /var/config/config.manageddata.spam.blacklist --from-s3 /data/config/manageddata/spam/blacklist --aws-key-file /etc/configs_readonly.conf -m config -p /config/manageddata/spam/blacklist'
        config_zk_path = "/config/manageddata/spam/blacklist"
        name = "config.manageddata.spam.blacklist"
        generated_metaconfig_content = metaconfig_manager.construct_metaconfig_with_single_section(
            name, config_zk_path, download_command, is_serverset=False
        )
        expected_metaconfig_content = simplejson.dumps(
            [
                {
                    "config_section_name": "config.manageddata.spam.blacklist",
                    "zk_path": "/config/manageddata/spam/blacklist",
                    "max_wait_in_secs": 0,
                    "command": "zk_download_data.py -f /var/config/config.manageddata.spam.blacklist --from-s3 /data/config/manageddata/spam/blacklist --aws-key-file /etc/configs_readonly.conf -m config -p /config/manageddata/spam/blacklist",
                    "type": "config"
                }
            ]
        )
        self.assertEqual(expected_metaconfig_content, generated_metaconfig_content)
