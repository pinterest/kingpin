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

from kingpin.kazoo_utils import KazooClientManager
from mock_kazoo_client import MockKazooClient


def initialize_kazoo_client_manager(zk_hosts):
    """ Initialize kazoo client manager. Note that all unit tests in kazoo_utils should
     use the initialization here."""
    kazoo_manager = KazooClientManager(zk_hosts,
                                       max_num_consecutive_failures=1,
                                       health_check_interval=0.0)
    kazoo_manager._client_callbacks = []


def get_mock_kazoo_client(*args, **kwargs):
    """Return a different mock kazoo client every time."""
    return MockKazooClient(hosts=kwargs['hosts'])


