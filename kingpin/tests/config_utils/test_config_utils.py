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

from unittest import TestCase
from kingpin import config_utils


class PathConversionTestCase(TestCase):

    example_config_path = '/var/config/config.manageddata.spam.domain_hidelist'
    example_zk_path = '/config/manageddata/spam/domain_hidelist'

    def test_get_config_file_path(self):
        self.assertEquals(
            self.example_config_path,
            config_utils.get_config_file_path(self.example_zk_path))

    def test_get_zk_path(self):
        self.assertEquals(
            self.example_zk_path,
            config_utils.get_zk_path(self.example_config_path))
