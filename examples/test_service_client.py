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


from kingpin.thrift_utils.thrift_client_mixin import PooledThriftClientMixin
from kingpin.thrift_utils.base_thrift_exceptions import ThriftConnectionError
from kingpin.kazoo_utils.hosts import HostsProvider

import TestService


class TestServiceConnectionException(ThriftConnectionError):
    pass


class TestServiceClient(TestService.Client, PooledThriftClientMixin):
    def get_connection_exception_class(self):
        return TestServiceConnectionException

testservice_client = TestServiceClient(
    HostsProvider([], file_path="/var/serverset/discovery.test_service.prod"),
    timeout=3000,
    pool_size=10,
    always_retry_on_new_host=True)

print testservice_client.ping()
