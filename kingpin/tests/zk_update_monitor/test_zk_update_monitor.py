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

from unittest import TestCase
import os
try:
    import simplejson as json
except ImportError:
    import json

from kingpin.zk_update_monitor import zk_util
from kingpin.zk_update_monitor.zk_update_monitor import transform_command_with_value

LARGE_SERVERSET_EXAMPLE_FILE_PATH = 'kingpin/tests/zk_update_monitor/large_serverset'
SMALL_SERVERSET_EXAMPLE = '10.168.233.131:9107,10.181.110.62:9145,10.233.95.194:9142,10.123.198.226:9130,10.239.174.184:9131'


class ZKUpdateMonitorTestCase(TestCase):

    """ Test cases for transforming zk_download_data command basing
        on different size of serversets
        Small serverset should pass in cmd line directly,
        large serverset should be written to a temp file
        """
    def test_transform_command_for_large_serverset(self):
        try:
            command = '/usr/local/bin/zk_download_data.py -f /var/serverset/discovery.stingray_dsl_mapper.prod -p /discovery/stingray_dsl_mapper/prod -m serverset'
            notification_timestamp = 1426859717.707331
            with open(LARGE_SERVERSET_EXAMPLE_FILE_PATH) as f:
                large_serverset_value = f.readline()
            (transformed_command, tmp_filepath) = transform_command_with_value(
                command, large_serverset_value, notification_timestamp)

            santinized_value = large_serverset_value.strip('\n').strip('\r')

            expected_tmp_filepath = '/tmp/zk_update_largefile_' + zk_util.get_md5_digest(
                santinized_value) + '_' + str(notification_timestamp)
            expected_transformed_command = '/usr/local/bin/zk_download_data.py -l ' \
                                           + expected_tmp_filepath + ' -f /var/serverset/discovery.stingray_dsl_mapper.prod -p /discovery/stingray_dsl_mapper/prod -m serverset'
            self.assertEqual(expected_transformed_command, transformed_command)
            self.assertEqual(expected_tmp_filepath, tmp_filepath)

            # Validate the file content
            tmpfile = open(expected_tmp_filepath)
            tmpfile_value = tmpfile.readline().rstrip('\n')
            tmpfile_md5 = tmpfile.readline()

            expected_tmpfile_value = santinized_value
            expected_tmpfile_md5 = zk_util.get_md5_digest(expected_tmpfile_value)

            self.assertEqual(expected_tmpfile_value, tmpfile_value)
            self.assertEqual(expected_tmpfile_md5, tmpfile_md5)
        finally:
            if os.path.isfile(tmp_filepath):
                os.remove(tmp_filepath)

    def test_transform_command_for_small_serverset(self):
        command = '/usr/local/bin/zk_download_data.py -f /var/serverset/discovery.stingray_dsl_mapper.prod -p /discovery/stingray_dsl_mapper/prod -m serverset'
        notification_timestamp = 1426859717.707331
        small_serverset_value = SMALL_SERVERSET_EXAMPLE
        (transformed_command, tmp_filepath) = transform_command_with_value(
            command, small_serverset_value, notification_timestamp)

        expected_tmp_filepath = None
        expected_transformed_command = '/usr/local/bin/zk_download_data.py -v ' \
                                       + "'" + small_serverset_value + "'" + ' -f /var/serverset/discovery.stingray_dsl_mapper.prod -p /discovery/stingray_dsl_mapper/prod -m serverset'
        self.assertEqual(expected_transformed_command, transformed_command)
        self.assertEqual(expected_tmp_filepath, tmp_filepath)


class SplitProblematicEndpointsLineTestCase(TestCase):

    def assert_splitted_parts_equal(self, desired_list, actual_list):
        desired_list.sort()
        actual_list.sort()
        self.assertEqual(desired_list, actual_list)

    def test_split_problematic_endpoints_line(self):
        line = "10.99.184.69:9001"
        splitted_parts = \
            zk_util.split_problematic_endpoints_line(line)
        self.assert_splitted_parts_equal(["10.99.184.69:9001"], splitted_parts)

        line = "10.99.184.69:900010.37.170.125:9006"
        splitted_parts = zk_util.split_problematic_endpoints_line(line)
        desired_answer = ["10.99.184.69:9000", "10.37.170.125:9006"]
        self.assert_splitted_parts_equal(desired_answer, splitted_parts)

        line = "10.99.184.69:900010.37.170.125:900610.99.184.69:9008"
        splitted_parts = zk_util.split_problematic_endpoints_line(line)
        desired_answer = ["10.99.184.69:9000", "10.37.170.125:9006", "10.99.184.69:9008"]
        self.assert_splitted_parts_equal(desired_answer, splitted_parts)

        line = ""
        splitted_parts = zk_util.split_problematic_endpoints_line(line)
        desired_answer = []
        self.assert_splitted_parts_equal(desired_answer, splitted_parts)

        line = "10.182.62.251:870310.186.44.155:9020"
        splitted_parts = zk_util.split_problematic_endpoints_line(line)
        desired_answer = ["10.182.62.251:8703", "10.186.44.155:9020"]
        self.assert_splitted_parts_equal(desired_answer, splitted_parts)

        line = "randomserver:9888randomserver2:9999"
        splitted_parts = zk_util.split_problematic_endpoints_line(line)
        desired_answer = ["randomserver:9888", "randomserver2:9999"]
        self.assert_splitted_parts_equal(desired_answer, splitted_parts)

        line = "1.1.1.1:1234255.255.255.255:4321"
        splitted_parts = zk_util.split_problematic_endpoints_line(line)
        desired_answer = ["1.1.1.1:1234", "255.255.255.255:4321"]
        self.assert_splitted_parts_equal(desired_answer, splitted_parts)
