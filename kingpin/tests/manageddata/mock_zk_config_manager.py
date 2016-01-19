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

import string


class MockZkConfigManager(object):
    def __init__(self, zk_hosts, aws_keyfile, s3_bucket, file_path, read_config_callback, s3_endpoint="s3.amazonaws.com", zk_path_suffix=''):
        assert hasattr(read_config_callback, '__call__')
        self.file_path = file_path
        self.read_config_callback = read_config_callback
        self._data = ""
        self.version = 0
        self.mock_timestamp = 0  # using logic clock for mocking.
        self.zk_hosts = zk_hosts
        self.aws_keyfile = aws_keyfile
        self.s3_bucket = s3_bucket

    def reload_config_data(self):
        if not self._data and 'messageconfigs' in self.file_path:
            managed_file = open(self.file_path, 'r')
            file_contents = string.join(managed_file.readlines(), ' ')
            self.read_config_callback(file_contents)
        else:
            self.read_config_callback(self._data)

    def update_zk(self, old_value, value, force_update=False):
        if not force_update and self._data and old_value != self._data:
            return False
        self._data = value
        self.read_config_callback(self._data)
        self.version += 1
        return True
