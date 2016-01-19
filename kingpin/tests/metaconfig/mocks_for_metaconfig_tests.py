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


class MockZkBaseConfigManager(object):
    def __init__(self, zk_hosts, zk_path, aws_keyfile, s3_bucket, s3_endpoint="s3.amazonaws.com"):
        self._data = ""
        self.version = 0
        self.mock_timestamp = 0  # using logic clock for mocking.
        self.zk_hosts = zk_hosts
        self.aws_keyfile = aws_keyfile
        self.s3_bucket = s3_bucket
        self.zk_path = zk_path

    def get_data(self):
        return "dummy data"

    def update_zk(self, old_value, value, force_update=True):
        return True

