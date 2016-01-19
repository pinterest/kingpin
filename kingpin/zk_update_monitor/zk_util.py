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

import hashlib
import logging
import re
import signal
import os

from kingpin.kazoo_utils import KazooClientManager

log = logging.getLogger(__name__)


def get_md5_digest(zk_data):
    return hashlib.md5(zk_data).hexdigest()


def get_md5_hash_sum(zk_data):
    hexdigest = get_md5_digest(zk_data)
    return sum(map(int, str(int(hexdigest, 16))))


def split_problematic_endpoints_line(line):
    """
    If the line of host contains more than one ":",
    for example: 10.99.184.69:900010.37.170.125:9006
    this splits the line and return a list of correct endpoints

    Args:
        ``line``: the problemtic line which contains more than one endpoint string.

    Returns:
        the splitted list of the problematic line which has correct endpoint strings.
    """

    colon_parts = line.strip().split(":")
    offset = len(colon_parts[-1])
    colon_positions = [m.start() for m in re.finditer(':', line)]
    start = 0
    split_parts = []
    for colon_position in colon_positions:
        end = colon_position + offset + 1
        split_part = line[start:end]
        split_parts.append(split_part)
        start = end
    return split_parts


def construct_s3_path(s3_key, timestamp):
    return '%s_%s' % (s3_key, timestamp)


def _kazoo_client(zk_hosts):
    return KazooClientManager(zk_hosts).get_client()


def _zk_path_exists(zk_hosts, path):
    # KazooClientManager is a singleton per cluster so we don't need
    # to be concerned about unnecessary object creation.
    kazoo = KazooClientManager(zk_hosts)
    return kazoo.get_client().exists(path)


def _kill(message):
    # Kill the ZUM.
    log.info(message)
    pid = os.getpid()
    os.kill(pid, signal.SIGKILL)


def parse_zk_hosts_file(zk_hosts_file_path):
    try:
        with open(zk_hosts_file_path, "r") as zk_hosts_file:
            zk_hosts = []
            for line in zk_hosts_file:
                if line:
                    zk_hosts.append(line.strip())
        return zk_hosts
    except:
        return None