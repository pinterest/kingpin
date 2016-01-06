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

"""
This script is for downloading the data config from zookeeper or s3.

In the "config" mode(specified by --mode or -m), this script can either download data directly from
zookeeper(config v1), or get the data from zookeeper node(which is the suffix for the s3 path) and
download the data from the concatenated s3 path(config v2).

"""

from gevent import monkey; monkey.patch_all()

import argparse
import datetime
import logging
import math
import os
import re
import requests
import string
import tempfile
import time

import gevent
from kazoo.exceptions import NoNodeError

from kingpin.config_utils import s3config
from kingpin.kazoo_utils import KazooClientManager
from kingpin.logging_utils import initialize_logger
from kingpin.zk_update_monitor import zk_util

log = logging.getLogger(__name__)

#########################################
### The zookeeper, aws configurations ###
#########################################
ZK_HOSTS = None
S3_BUCKET = None
AWS_KEY_FILE = None
S3_ENDPOINT = None

#############################################
### Endpoints of Flask app running in ZUM ###
#############################################
_ZK_UPDATE_MONITOR_FLASK_PORT = None
_METADATA_REPORT_ENDPOINT = None
_FILE_MODIFICATION_REPORT_ENDPOINT = None
_QUERY_SERVERSET_TYPE_ENDPOINT = None
_REPORT_SERVERSET_TYPE_ENDPOINT = None

# When downloading a serverset, if the count of servers in the new serverset
# is smaller than the ratio below of the server count in the existing target
# file, the new serverset will be rejected and not written to the target file.
DEFAULT_SERVERSET_REJECTION_RATIO = 0.5

# When downloading a serverset, if the count of servers falls under this number,
# ZK_UPDATE_MONITOR will reject downloading and updating the serverset.
DEFAULT_SERVERSET_MINIMUM_SIZE = 0

# The minimum server count in the target file for applying the rejection logic,
# when the existing server set is too small, we won't reject the server set.
# The exception is that if the new serverset is empty, we will reject it as
# long as the existing host set is not empty.
DEFAULT_MIN_SERVER_COUNT_FOR_REJECTION_RATIO = 5

# When downloading a new config or serverset from zookeeper during refresh,
# if the local file has a different content and the last update time is more
# than the number of minutes before the current time, the script alerts for
# the stale file content.
_STALE_FILE_THRESHOLD_IN_MINUTES = 10

# The zookeeper notification may have a latency to reach the daemon. So before
# checking for stale content, wait for some time to ensure we don't alert
# when a notification is pending.
#
_WAIT_TIME_BEFORE_STALE_CHECK_IN_SECS = 60

_FAILED_TO_GET_DATA_FROM_ZK_CODE = 2

_TOO_FEW_SERVERS_IN_ZK_CODE = 3

_SERVER_COUNT_DROP_BELOW_THRESHOLD = 30

_STALE_LOCAL_FILE_CODE = 4

_UNRECOGNIZED_MODE_CODE = 5

_ZK_PATH_NOT_EXIST_CODE = 6

_UNHANDLED_EXCPETION = 7

_S3_VALUE_ERROR_CODE = 101

_S3_ERROR_CODE = 102

_NO_AWS_KEY_FILE = 8

# When the local file has the same content as zookeeper, the script exits with
# the special code below. The zk_update_monitor daemon ignores this code.
#
_SAME_CONTENT_CODE = 123

# When read from a local file and the first line (actual data) doesn't match
# with the second line (md5 sum), or there are less than 2 lines, this error
# code will be returned.
_BROKEN_VALUE_FILE = 9

_METADATA_REPORT_MAX_RETRY_COUNT = 3


def _get_host_name_and_port(host_data):
    #example data:
    #/discovery/userfeed/prod/69ca9daa191e40-userfeed-8a4e2be7:8707
    #result should be userfeed-8a4e2be7:8707
    pattern = r'^\w+-([\w-]+:\d+)$'
    match = re.search(pattern, host_data)
    if match:
        return match.group(1)
    else:
        return None


def _get_host_ip_and_port(host_data):
    #example data:
    #/discovery/ngapp/prod/2890a90311ce4d8aa0656bf1650d7e6c-10.86.5.55:80
    #result should be 10.86.5.55:8707
    pattern = r'^\w+-([\d.]+:\d+)$'
    match = re.search(pattern, host_data)
    if match:
        return match.group(1)
    else:
        return None


def _get_host_data_from_zknode(kazoo_client, host_data):
    # host_data is a zkpath to look up.  if it's present and contains
    # non-empty data, use that.
    try:
        data, stat = kazoo_client.get(host_data)
    except NoNodeError:
        return None
    if data is None or data == "":
        return None
    else:
        return data


def _get_hosts_from_zk(kazoo_client, zk_path):
    # To get serverset, the znode of the zk_path needs to be a
    # directory and all its children need to be ephemeral file
    # nodes containing the host information.
    hosts_data = kazoo_client.get_children(zk_path)
    hosts = set()
    for host_data in hosts_data:
        # Data in zk may use host name or ip, so need to try both formats.
        host_name = _get_host_name_and_port(host_data)
        if host_name:
            hosts.add(host_name)
        else:
            host_ip = _get_host_ip_and_port(host_data)
            if host_ip:
                hosts.add(host_ip)
            else:
                log.error("Cannot process host data %s" % host_data)

    log.debug("Found %d registered hosts in zookeeper from path %s"
              % (len(hosts), zk_path))
    return '\n'.join(hosts)


def get_data_from_zk(zk_path, mode):
    kazoo_client = KazooClientManager(ZK_HOSTS).get_client()

    if not kazoo_client.exists(zk_path):
        log.error("Path %s does not exist" % (zk_path))
        exit(_ZK_PATH_NOT_EXIST_CODE)

    try:
        if mode == "CONFIG":
            value, stat = kazoo_client.get(zk_path)
            return value, stat.version
        elif mode == 'SERVERSET':
            value = _get_hosts_from_zk(kazoo_client, zk_path)
            return value, None
        else:
            log.error("Unrecognized mode: %s" % mode)
            exit(_UNRECOGNIZED_MODE_CODE)
    except:
        log.error("Failed to get value from zookeeper path %s with mode %s"
                  % (zk_path, mode))
        exit(_FAILED_TO_GET_DATA_FROM_ZK_CODE)


def _get_server_count(serverset_content):
    if not serverset_content:
        return 0
    return serverset_content.strip('\n').count('\n') + 1


def _same_content(existing_content, zk_data, mode):
    if existing_content is None:
        # When local file does not exist, allow empty zk data to be
        # used for creating a file.
        return zk_data is None
    if not existing_content and not zk_data:
        return True
    if not existing_content or not zk_data:
        return False
    if mode == 'SERVERSET':
        def not_empty(x): return x
        existing_serverset = \
            set(filter(not_empty, existing_content.split('\n')))
        zk_serverset = set(filter(not_empty, zk_data.split('\n')))
        return existing_serverset == zk_serverset
    else:
        return existing_content == zk_data


def _check_stale_file_content(existing_content, file_path, zk_data, mode):
    if (existing_content is None or
        _same_content(existing_content, zk_data, mode)):
        return
    gevent.sleep(_WAIT_TIME_BEFORE_STALE_CHECK_IN_SECS)
    mtime = datetime.datetime.utcfromtimestamp(
        os.path.getmtime(file_path))
    now = datetime.datetime.utcnow()
    # We compare the file's mtime with the current time to decide whether
    # the file was stale. Since we don't hit this code if the same file
    # was updated within the last 30 seconds from zk_update_monitor, the
    # chance for a race condition is small.
    # TODO: consider to get the the actual mtime from zookeeper if false
    # alerts become an issue.
    diff_in_minutes = (now - mtime).seconds / 60
    if _STALE_FILE_THRESHOLD_IN_MINUTES < diff_in_minutes:
        log.error("File %s is %d minutes behind zookeeper"
                  % (file_path, diff_in_minutes))
        return False
    return True


def check_flappy_serverset(zk_serverset, existing_serverset,
                           rejection_ratio, serverset_minimum_size,
                           file_path):
    # If the serverset has a radical change, we assume this is unusual
    # and does not update the local serverset file content.
    existing_count = _get_server_count(existing_serverset)
    new_count = _get_server_count(zk_serverset)

    if new_count < serverset_minimum_size:
        log.error("Serverset %s has %d hosts, "
                  "falling below its minimum allowed size %d." % (
            file_path, existing_count, serverset_minimum_size))
        exit(_SERVER_COUNT_DROP_BELOW_THRESHOLD)

    if ((existing_count >= DEFAULT_MIN_SERVER_COUNT_FOR_REJECTION_RATIO and
        new_count < existing_count * rejection_ratio) or
       (existing_count > 0 and new_count == 0)):
        log.error("Serverset in zk contains %d servers and is smaller "
                  "than %d%% of count of servers in existing file %s "
                  "of %d servers rejecting the new serverset"
                  % (new_count, rejection_ratio*100,
                     file_path, existing_count))
        exit(_TOO_FEW_SERVERS_IN_ZK_CODE)


def report_metadata(mode, zk_data, zk_path, version, notification_timestamp,
                    modification_timestamp, retry_count=0):
    if not zk_path:
        return

    if mode == 'SERVERSET':
        hashsum = zk_util.get_md5_hash_sum(zk_data)
        try:
            num_endpoints = len(zk_data.split(','))
        except:
            # A negative value means there are problems getting the count of endpoints
            num_endpoints = -1
        postbody = {
            'hashsum': hashsum,
            'num_endpoints': num_endpoints,
            'zk_path': zk_path,
            'notification_timestamp': notification_timestamp,
            'modification_timestamp': modification_timestamp,
            'mode': mode
        }
    else:
        postbody = {
            'version': version,
            'zk_path': zk_path,
            'notification_timestamp': notification_timestamp,
            'modification_timestamp': modification_timestamp,
            'mode': mode
        }
    try:
        response = requests.post(_METADATA_REPORT_ENDPOINT, data=postbody)
        if response.status_code != 200:
            log.error("Failed to get response with 200 code posting metadata for %s: %d",
                      zk_path, response.status_code)
            if retry_count < _METADATA_REPORT_MAX_RETRY_COUNT:
                report_metadata(
                    mode, zk_data, zk_path, version, notification_timestamp,
                    modification_timestamp, retry_count+1)
            else:
                log.error("Post metadata of %s retries all used but still fail.", zk_path)
    except:
        log.exception("Fail to post metadata to zk_update_monitor for %s", zk_path)
        if retry_count < _METADATA_REPORT_MAX_RETRY_COUNT:
            report_metadata(
                mode, zk_data, zk_path, version, notification_timestamp,
                modification_timestamp, retry_count+1)


def report_file_modification(mode, zk_path,
                             modification_timestamp, retry_count=0):
    postbody = {
        'zk_path': zk_path,
        'modification_timestamp': modification_timestamp,
        'mode': mode
    }

    # It is possible the report fails when flask server is not up and running,
    # and report_file_modification connection fails.
    # This case happens when a config is updated during the window when zk_update_monitor
    # is being restarted, so it will cause a false negative that the config modification time is lagging behind.
    # So we do some sleep in retry, waiting flask server to be up and report again will succeeded.
    backoff_seconds = math.pow(3, retry_count + 1)

    try:
        response = requests.post(_FILE_MODIFICATION_REPORT_ENDPOINT, data=postbody)
        if response.status_code != 200:
            log.error("Failed to get response with 200 code posting modification timestamp "
                      "for %s: %d", zk_path, response.status_code)
            if retry_count < _METADATA_REPORT_MAX_RETRY_COUNT:
                log.error("report_file_modification sleeps %d seconds before retry", backoff_seconds)
                gevent.sleep(backoff_seconds)
                report_file_modification(mode, zk_path, modification_timestamp, retry_count+1)
    except:
        log.exception("Fail to post modification timestamp to zk_update_monitor for %s", zk_path)
        if retry_count < _METADATA_REPORT_MAX_RETRY_COUNT:
            log.error("report_file_modification sleeps %d seconds before retry", backoff_seconds)
            gevent.sleep(backoff_seconds)
            report_file_modification(mode, zk_path, modification_timestamp, retry_count+1)


def verify_and_correct_endpoints_set(endpoints_set, downloaded_file):
    """
    There were some cases where the endpoint is in a wrong format, for example:
        10.182.62.251:870310.186.44.155:9020
    It is probably because the line contains 2 endpoints appended together.
    This method fixes the issue by checking the number of colons of each
    endpoint, and if there are more/less than 1 colon,
    calls zk_util.split_problematic_endpoints_line to fix and put correct
    one back to the endpoints_set.

    Args:
        ``endpoints_set``: the set which has a list of endpoints of serverset.
        ``downloaded_file``: the path of the file the serverset to be downloaded to.

    Returns:
        verified and corrected endpoints_set.
    """
    problematic_lines = []
    for endpoint in endpoints_set:
        if endpoint.count(":") != 1:
            # Log it
            log.error("Incorrect endpoint format: %s, file: %s",
                      endpoint, downloaded_file)
            problematic_lines.append(endpoint)
    if len(problematic_lines) == 0:
        return

    for problematic_line in problematic_lines:
        endpoints_set.remove(problematic_line)
        corrected_hosts = zk_util.split_problematic_endpoints_line(
            problematic_line)
        endpoints_set.update(corrected_hosts)


def initialize_zookeeper_aws_s3_configs(args):
    global ZK_HOSTS
    global AWS_KEY_FILE
    global S3_BUCKET
    global S3_ENDPOINT

    ZK_HOSTS = zk_util.parse_zk_hosts_file(args.zk_hosts_file)
    AWS_KEY_FILE = args.aws_key_file
    S3_ENDPOINT = args.s3_endpoint
    S3_BUCKET = args.s3_bucket


def initialize_zk_update_monitor_endpoints(port):
    # The REST endpoint in zk_update_monitor which is used to
    # send metadata to.
    global _ZK_UPDATE_MONITOR_FLASK_PORT
    global _METADATA_REPORT_ENDPOINT
    global _FILE_MODIFICATION_REPORT_ENDPOINT
    global _QUERY_SERVERSET_TYPE_ENDPOINT
    global _REPORT_SERVERSET_TYPE_ENDPOINT

    _ZK_UPDATE_MONITOR_FLASK_PORT = int(port)
    _METADATA_REPORT_ENDPOINT = 'http://localhost:%d/admin/report_metadata' % \
                                _ZK_UPDATE_MONITOR_FLASK_PORT
    _FILE_MODIFICATION_REPORT_ENDPOINT = 'http://localhost:%d/admin/report_file_modification' \
                                         % _ZK_UPDATE_MONITOR_FLASK_PORT
    _QUERY_SERVERSET_TYPE_ENDPOINT = "http://localhost:%d/admin/query_serverset_type" \
                                     % _ZK_UPDATE_MONITOR_FLASK_PORT
    _REPORT_SERVERSET_TYPE_ENDPOINT = "http://localhost:%d/admin/report_serverset_type" \
                                      % _ZK_UPDATE_MONITOR_FLASK_PORT


def main():
    global args
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-f", "--file", dest="file", metavar="FILENAME", required=True,
        help="The target file name for storing the downloaded data.",)
    parser.add_argument(
        "-p", "--path", dest="path", metavar="PATH",
        help="The zookeeper path to download data from")
    parser.add_argument(
        "-m", "--mode", dest="mode", metavar="MODE", required=True,
        help="The download mode [config | serverset]")
    parser.add_argument(
        "-s", "--show", dest="show", action='store_true',
        help="Flag to indicate whether to show the value in zookeeper only, "
             "no change is really made when it is set.")
    parser.add_argument(
        "--allow-same-content", dest="allow_same_content",
        action='store_true',
        help="Flag to indicate whether the script thinks downloading the same "
             "content is OK, no setting this flag will make the script exit "
             "with non-zero code when the content in zookeeper is the same as "
             "the target file.")
    parser.add_argument(
        "-r", "--serverset-rejection-ratio", dest="rejection_ratio",
        metavar="RATIO", default=DEFAULT_SERVERSET_REJECTION_RATIO, type=float,
        help="The ratio to reject a new serverset, if the count of servers "
             "in the new serverset is smaller than the multiplication of the "
             "count of servers in the existing target file and the ratio, the "
             "new serverset will be rejected and not written to the target"
             "file, default is %.2f." % DEFAULT_SERVERSET_REJECTION_RATIO)
    parser.add_argument(
        "--serverset-minimum-size", dest="serverset_minimum_size",
        metavar="MINSIZE", default=DEFAULT_SERVERSET_MINIMUM_SIZE, type=int,
        help="The minimum size of a serverset. If the serverset's size ever"
             "falls below this min size, zk_update_monitor will reject the update"
             "and will not write to the target file. Default is %d."
             % DEFAULT_SERVERSET_MINIMUM_SIZE)
    parser.add_argument(
        "-v", "--value", dest="value", metavar="VALUE",
        help="The value in zookeeper, once this is provided, the script will "
             "not try to get data from zookeeper, instead it will use this "
             "provided value to write to the file. For config mode, the value "
             "is the same as the data in zookeeper, for serverset mode, the "
             "value is a list of hosts separated by ','")
    parser.add_argument(
        "--from-s3", dest="s3_key", metavar="S3_KEY",
        help="The S3 key to download the data from")
    parser.add_argument(
        "--report-metadata", dest="report_metadata",
        default=False, action='store_true',
        help="If enabled, the downloader script "
             "will report metadata of serverset"
             "or config back to the caller (zk_download_data.py)"
    )
    parser.add_argument(
        "-l", "--from-value-file", dest="from_value_file",
        help="To get the actual value from a file"
    )
    parser.add_argument(
        "-o", "--port", dest="port", metavar="PORT",
        help="The port the flask app in ZUM is running on"
    )
    ######## REQUIRED #########
    # ZK / S3 Configurations. #
    ###########################
    parser.add_argument(
        "-z", "--zk-hosts-file-path", dest="zk_hosts_file", metavar="ZKHOSTS",
        required=True,
        help="The path of file which have a list of Zookeeper endpoints "
             "(host:port) which keeps the metaconfig as well as "
             "the config/serversets"
    )
    parser.add_argument(
        "-a", "--aws-key-file", dest="aws_key_file", metavar="AWSKEY",
        help="The path of the file storing AWS access and secret keys",
    )
    parser.add_argument(
        "-b", "--s3-bucket", dest="s3_bucket", metavar="BUCKET",
        help="The S3 bucket storing metaconfigs / configs"
    )
    parser.add_argument(
        "-e", "--aws-s3-endpoint", dest="s3_endpoint", metavar="ENDPOINT",
        default="s3.amazonaws.com",
        help="The S3 endpoint storing metaconfig / configs"
    )

    args = parser.parse_args()
    mode = args.mode.upper()
    initialize_zookeeper_aws_s3_configs(args)
    initialize_zk_update_monitor_endpoints(args.port)

    version = None
    notification_timestamp = time.time()
    # this is used to calculate MD5 which is consistent with the santinized
    # value in zk_update_monitor
    santinized_zk_data = None

    if mode == 'CONFIG' and args.s3_key:
        if not AWS_KEY_FILE or not S3_BUCKET or not S3_ENDPOINT:
            log.error("No AWS key file or S3 Config is provided for accessing S3.")
            exit(_NO_AWS_KEY_FILE)
        log.info("Downloading from s3 key %s in bucket %s"
                 % (args.s3_key, S3_BUCKET))

        # We use the s3_key (which is actually a prefix) concatenated with the value from zk node to
        # form the s3 path. Therefore, each time we update new data to s3, we create a new S3
        # key(can be located from the zk_node value), so as to make use of the "almost"
        # read-after-create guarantee from the s3 "special" url.
        zk_value = args.value
        if zk_value:
            log.debug("Use provided value %s" % zk_value)
        else:
            zk_value, version = \
                get_data_from_zk(args.path, mode)

        s3_path = zk_util.construct_s3_path(args.s3_key, zk_value)

        # Download data from the s3 path.
        try:
            zk_data = s3config.S3Config(AWS_KEY_FILE, S3_BUCKET, s3_endpoint=S3_ENDPOINT).get_config_string(s3_path)
        except ValueError as ve:
            # If the s3 key specified by the zk_node does not exist, this is probably due to s3
            # read-after-write inconsistency. Abort updating the local file.
            log.error("Abort downloading from s3 key %s due to ValueError: %s" % (s3_path, str(ve)))
            exit(_S3_VALUE_ERROR_CODE)
        except Exception as e:
            log.error("Abort downloading from s3 key %s due to unexpected s3 exception: %s" %
                      (s3_path, str(e)))
            exit(_S3_ERROR_CODE)
    else:
        if args.value:
            log.debug("Use provided value %s" % args.value)
            if mode == 'SERVERSET':
                # remove duplicates in the server set value
                endpoints_set = set(args.value.split(','))
                verify_and_correct_endpoints_set(endpoints_set, args.file)
                zk_data = '\n'.join(endpoints_set)
            else:
                zk_data = args.value
        elif args.from_value_file:
            from_value_file = args.from_value_file
            log.debug("Use provided value in %s" % args.from_value_file)
            try:
                with open(from_value_file, 'r') as f:
                    # verify the file content is good.
                    value = f.readline().rstrip('\n')
                    md5digest = f.readline()
                    calculated_md5digest = zk_util.get_md5_digest(value)
                    if calculated_md5digest != md5digest:
                        log.error("Temp file %s content does not match md5" % from_value_file)
                        f.close()
                        exit(_BROKEN_VALUE_FILE)
                if mode == 'SERVERSET':
                    endpoints_set = set(value.split(','))
                    verify_and_correct_endpoints_set(endpoints_set, args.file)
                    zk_data = '\n'.join(endpoints_set)
                else:
                    zk_data = value
            except:
                log.error("Error reading from temp file %s" % from_value_file)
                exit(_BROKEN_VALUE_FILE)
        else:
            zk_data, version = \
                get_data_from_zk(args.path, mode)
            if zk_data is None:
                log.error("Failed to get data from zookeeper.")
                exit(_FAILED_TO_GET_DATA_FROM_ZK_CODE)

    if args.show:
        print(zk_data)
        return

    log.debug("Target file = %s" % (args.file))

    try:
        with open(args.file, "r") as file:
            existing_content = file.read()
    except IOError:
        existing_content = None

    if mode == 'SERVERSET':
        check_flappy_serverset(zk_data, existing_content,
                               args.rejection_ratio,
                               args.serverset_minimum_size, args.file)

    if args.report_metadata:
        if mode == 'SERVERSET':
            serverset_list = zk_data.split('\n')
            serverset_list.sort()
            santinized_zk_data = string.join(serverset_list, ',')

        # Get the file modification time
        if args.file and os.path.exists(args.file):
            modification_timestamp = os.path.getmtime(args.file)
        else:
            modification_timestamp = 0

        report_metadata(mode, santinized_zk_data, args.path, version,
                        notification_timestamp, modification_timestamp)

    if _same_content(existing_content, zk_data, mode):
        log.warn("The data in zookeeper is the same as the target file %s, "
                 "ignore." % args.file)
        if not args.allow_same_content:
            # Needs to exit with the special return code so the daemon can
            # ignore it.
            exit(_SAME_CONTENT_CODE)
        else:
            exit(0)

    found_stale_content = False
    if os.environ.get('__ZK_UPDATE_MONITOR_REFRESH__', False):
        found_stale_content = \
            _check_stale_file_content(existing_content, args.file,
                                      zk_data, mode)

    log.info("Generating file %s with the data from zookeeper..."
             % (args.file))

    tmp_dir = tempfile.gettempprefix()
    tmp_filename = os.path.join('/', tmp_dir, args.file.replace("/", "_"))

    try:
        with open(tmp_filename, 'w') as f:
            f.write(zk_data)
    except:
        log.exception("Failed to generate file %s from config in zookeeper. "
                      "Data to write: %s" % (tmp_filename, zk_data))
        exit(_UNHANDLED_EXCPETION)

    os.rename(tmp_filename, args.file)
    log.info("File is generated.")

    # Report the file modification time.
    report_file_modification(mode, args.path, time.time())

    if found_stale_content:
        exit(_STALE_LOCAL_FILE_CODE)


if __name__ == "__main__":
    initialize_logger(logger_filename="zk_download_data.log")
    main()