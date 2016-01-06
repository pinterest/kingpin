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

""" An example Using Kazoo_utils to join a serversets.
"""

import argparse
from kingpin.zk_update_monitor import zk_util
from kingpin.kazoo_utils import ServerSet
import gevent

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MetaConfig Management Shell")
    parser.add_argument(
        "-p", "--port", dest="port", required=True,
        help="The port you want to register to the serverset"
    )
    parser.add_argument(
        "-z", "--zk-hosts-file-path", dest="zk_hosts_file", metavar="ZKHOSTS",
        required=True,
        help="The path of file which have a list of Zookeeper endpoints "
             "(host:port) which keeps the metaconfig as well as "
             "the config/serversets"
    )
    parser.add_argument(
        "-k", "--zk-path", dest="zk_path", metavar="ZKPATH",
        required=True,
        help="Zk Path of the serverset"
    )
    args = parser.parse_args()
    zk_hosts = zk_util.parse_zk_hosts_file(args.zk_hosts_file)

    g = ServerSet(args.zk_path, zk_hosts).join(int(args.port))

    gevent.sleep(100000)