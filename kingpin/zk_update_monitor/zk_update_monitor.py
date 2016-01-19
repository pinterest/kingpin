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

"""ZK_UPDATE_MONITOR (ZUM): A daemon program that monitors changes in zookeeper.

The module runs as a daemon to monitor zookeeper paths specified in a config
file, and invokes a command if a change to the path in zookeeper is observed.
It monitors zookeeper by using DataWatch in the ``kazoo_utils`` library.

There are 2 types of data zk_update_montior can watch, serverset and config.
Serveset is a group of Zookeeper Epemeral nodes whose name are endpoints of services.
Config are decider/manageddata whose values are stored in S3 and use Zookeeper as the notification
system to notify ZUM the updates.

The following is an example "MetaConfig" JSON which tells ZK_UPDATE_MONITOR to watch a serverset,
and fork zk_download_data.py when the content of a watched serverset is changed:

 [
    {
        "config_section_name": "discovery.service.prod",
        "zk_path": "/discovery/service/prod",
        "command": "zk_download_data.py -f /var/serverset/discovery.service.prod -p /discovery/service/prod -m serverset",
        "type": "serverset",
        "max_wait_in_secs": 0
    }
 ]


This is an example "MetaConfig" JSON which tells ZK_UPDATE_MONITOR to watch a configuration (manageddata)
and fork zk_download_data.py when the content of a watched mangeddata is changed. It downloads the data from S3.

 [
    {
        "config_section_name": "config.manageddata.spam.test_grey_list",
        "zk_path": "/config/manageddata/spam/test_grey_list",
        "command": "zk_download_data.py -f /var/config/config.manageddata.spam.test_grey_list --from-s3 /data/config/manageddata/spam/test_grey_list -m config -p /config/manageddata/spam/test_grey_list",
        "type": "config",
        "max_wait_in_secs": 0
    }
 ]

The ``max_wait_in_seconds`` specifies the max number of seconds the the daemon
waits before invoking the command. The actual wait time is a random number of
seconds between 0 to the ``max_wait_in_secs`` value. This random waiting is for preventing many
clients catch the same zookeeper change and run the command at the same time
that could possibly cause a service outage.

The MetaConfig can be created using in config_meta_manager.

How to launch zk_update_monitor:
See details in Kingpin README.md or comments in config_meta_manager.


How to monitor zk_update_monitor:
ZUM locally runs a flask that outsiders can query the status of the daemon. You can simply curl the local URLS
to get the information.

"""


from gevent import monkey; monkey.patch_all()

import argparse
import datetime
import functools
import logging
import os
import random
import socket
import string
try:
    import simplejson as json
except ImportError:
    import json
import tempfile
import time

from flask import Flask
from flask import request
from flask import jsonify
import gevent
from gevent import pywsgi
import gevent_subprocess as subprocess

from kingpin.config_utils import s3config
from kingpin.kazoo_utils import DataWatcher, ServerSet
from kingpin.logging_utils import initialize_logger
from kingpin.zk_update_monitor import zk_util
from kingpin.zk_update_monitor.zk_util import _kazoo_client, _zk_path_exists, _kill


log = logging.getLogger(__name__)
app = Flask(__name__)

# The following value specifies the wait time the daemon invokes all associated
# command lines. This way we can assure even for some reason the daemon misses
# an update from zookeeper (e.g. due to transient network issue), it still can
# get the latest data from zookeeper within a reasonable time period.
# Currently we fix the wait time to be 2 hours, we can allow configuring this
# through configuration or allowing different wait time for different monitored
# zookeeper paths, if needed.
#
WAKEUP_FREQUENCY_IN_SECS = os.getenv(
    "ZK_UPDATE_MONITOR_WAKEUP_FREQUENCY_IN_SECS", 1800)

MAX_JITTER_TIME_IN_SECS = os.getenv(
    "ZK_UPDATE_MONITOR_MAX_JITTER_TIME_IN_SECS", 180)

# We restart the daemon periodically to avoid the situation that the daemon
# gets stuck and cannot progress for unexpected reasons.
# Every time when the daemon wakes up,
# it generates a number between 0-1 to compare with the number below.
# If it is smaller than the number below, it will exit and supervisord will
# restart it. Currently we set the probability to 1/25, which is roughly one
# restart every half day with the daemon wakes up every 30 minutes.
RESTART_PROBABILITY = 0.04

_PATH_TO_DATA = {}
# Ex:  _PATH_TO_DATA = {
# "/config/nutcracker/conf_file": {"version": 2,
#                                  "notification_timestamp": 1234567,
#                                  "last_processed_timestamp": 1234567} }

_PATH_TO_COMMAND = {}

_SERVERSET_METADATA = {}
# Ex: _SERVERSET_METADATA = {
# "/discovery/followeronline/prod": {"hashsum": 12345678,
#                                    "notification_timestamp": 1234567,
#                                    "num_endpoints": 13} }

# To prevent from possible conflicts running the same command concurrently
# (e.g. the periodical refresh runs the same command at the same time), record
# the start time of a command triggered by zookeeper notification here. The
# periodical won't execute the same command if it is too close to the time.
#
_COMMAND_TO_NOTIFICATION_TIME = {}

# The periodical refresh won't start a command if the same command was
# triggered by zookeeper change within the number of seconds below.
#
_CONCURRENT_MIN_WINDOW_IN_SECS = 60


# We allow the daemon to ignore certain non-zero return codes so we can reserve
# a few codes for scenarios that we expect to handle by someone but are not
# considered as errors to trigger alerts here. E.g. when the local file and
# zookeeper have the same content, the download script returns non-zero code
# so the following command to restart the service does not have to run.
#
_IGNORED_RET_CODES = [123]


# Some return code seems to be temporary failures, retry can usually fix it.
_RETRY_RET_CODES = [1, 9, 101, 102]

_MAX_RETRY_COUNT = 1

_RETRY_WAIT_TIME_IN_SECONDS = 3

_TOO_FEW_SERVERS_IN_ZK_CODE = 3

_SERVER_COUNT_DROP_BELOW_THRESHOLD = 30

_UNHANDLED_EXCPETION = 7

_SERVERSET_REJECTION_ERROR_CODES = [3, 30]

# Using a queue to store all the notification events, a worker will pick events
# from the queue. This way we can avoid possible race conditions that an
# earlier notification is executed later to bring in stale data.
#
_NOTIFICATION_EVENT_QUEUE = gevent.queue.Queue()

# Used as a buffer to cache events which happens during the
# _DOWNLOAD_PROCESS_SPAWN_INTERVAL.
# There will be a dedicated worker which is woke up to dequeue events
# from this queue. The worker looks at the notification time of the events
# and decide whether the events should be put back to
# _NOTIFICATION_EVENT_QUEUE or ignored based on its
# notification timestamp and the last event notification timestamp of that
# config/serverset from _PATH_TO_DATA.
_NOTIFICATION_HOLDDOWN_QUEUE = gevent.queue.Queue()
# unit: second
_NOTIFICATION_HOLDDOWN_WIPER_SLEEP_INTERVAL_IN_SECONDS = 10

# Not every zk path needs trigger alerts when the associated command fails.
# The value is specified in config for the zookeeper path.
# The map below is from string to boolean.
_PATH_TO_ALERT_DISABLED = {}

_HOST_NAME = socket.gethostname()

_CONFIGS_WITH_NONEXISTENT_PATH = []

# every minute dump the command processing information in the format of
# (zk_path, command, last_time_command_invoked)
_DUMP_COMMAND_PROCESSING_INFO_INTERVAL_IN_SECONDS = 60

# If the value size is larger than this threshold, we will write the value
# to a temp file and change the download script to load value from the temp
# file.
_LONG_VALUE_THRESHOLD = 100000


# A dict for storing metadata of greenlets
_GREENLET_DICT = {}

# A flag indicating whether the intialization is completed
# Initialization is completed when all configs are loaded, but it is not guaranteed that
# all config's modification time are correct (some delay may be expected in zk_download_data).
_INITIALIZATION_COMPLETED = False

# Update rejected serversets
# Should we exclude stingray?
_REJECTED_UPDATED_SERVERSETS = set()

# A set keeping serversets which have 0 children and 0 cversion
_SKIP_FLAPPY_CHECK_SERVERSETS = set()

# A set keeping serversets which don't have 0 children or cversion not 0
_NON_SKIP_FLAPPY_CHECK_SERVERSETS = set()

# We have a wiper greenlet to invalidate the above 2 caches every 30 min.
_FLAPPY_CHECK_SERVERSETS_TTL = 30 * 60

# The interval which is enforced between 2 zk_download_data subprocesses for
# one same configuration/serverset. This is used to prevent very flapping
# configs/serversets that produce tons of notification to ZUM and causing
# tons of zk_download_data processes which will cause CPU usage very high.
# unit: seconds
_DOWNLOAD_PROCESS_SPAWN_INTERVAL_SECONDS = 1


#########################################
### The zookeeper, aws configurations ###
#########################################
ZK_HOSTS = None
ZK_HOSTS_FILE = None
S3_BUCKET = None
AWS_KEY_FILE = None
S3_ENDPOINT = None


######################################
###### MetaConfig Datastructures #####
######################################
# Templates for generating ZK paths of metaconfig/
METACONFIG_ZK_PATH_FORMAT = '/metaconfig/metaconfig/{}'
DEPENDENCY_ZK_PATH_FORMAT = '/metaconfig/dependency/{}'
METACONFIG_S3_KEY_FORMAT = "/data/metaconfig/metaconfig/{}"

_WATCHED_DEPENDENCIES = set()
_WATCHED_METACONFIGS = set()

# The string to be added into each zk_download_data command which contains
# zookeeper endpoints, s3 endpoints, s3 bucket and aws keyfile path.
EXTRA_FLAGS_FOR_ZK_DOWNLOAD_DATA = None

_CONFIGV3_INITIALIZED = False


def update_serverset_metadata(zk_path, notification_timestamp, value):
    hashsum = zk_util.get_md5_hash_sum(value)
    try:
        num_endpoints = len(value.split(','))
    except:
        # A negative value means there are problems getting the count of endpoints
        num_endpoints = -1
    metadata = {"hashsum": hashsum,
                "num_endpoints": num_endpoints,
                "notification_timestamp": notification_timestamp}
    if zk_path in _SERVERSET_METADATA:
        _SERVERSET_METADATA[zk_path].update(metadata)
    else:
        _SERVERSET_METADATA[zk_path] = metadata


def _check_download_interval(zk_path, current_time):
    last_processed_timestamp = \
        _PATH_TO_DATA[zk_path].get("last_processed_timestamp")
    if not last_processed_timestamp \
            or current_time - last_processed_timestamp > _DOWNLOAD_PROCESS_SPAWN_INTERVAL_SECONDS:
        _PATH_TO_DATA[zk_path].update(
            {'last_processed_timestamp': current_time})
        return True
    return False


def _is_older_version(zk_path, version, notification_timestamp):
    data = _PATH_TO_DATA.get(zk_path)
    if version:
        if (data and data.get("version", -1) > version):
            log.info(
                "Queued notification event has version %d which is not "
                "newer than the current version %d, ignore"
                % (version, data["version"])
            )
            return True
    else:
        # when version is missing (esp. in the server set case), use
        # the order that the notifications are received to decide which
        # one is older
        if data and data.get("notification_timestamp", 0) >= notification_timestamp:
            log.info(
                "Queued notification event has a timestamp %f which "
                "is not newer than the current timestamp %f, ignore"
                % (notification_timestamp, data["notification_timestamp"])
            )
            return True
    return False


def transform_command_with_value(command, value, notification_timestamp):
    python_download_script = 'zk_download_data.py'

    if len(value) > _LONG_VALUE_THRESHOLD:
        # If the value is too long (serverset is too large), OSError may be thrown.
        # Instead of passing it in command line, write to a temp file and
        # let zk_download_data read from it.
        value = value.replace("\n", "").replace("\r", "")
        md5digest = zk_util.get_md5_digest(value)
        tmp_filename = 'zk_update_largefile_' + md5digest + '_' + str(notification_timestamp)
        tmp_dir = tempfile.gettempprefix()
        tmp_filepath = os.path.join('/', tmp_dir, tmp_filename)

        log.info("This is a long value, write it to temp file %s", tmp_filepath)
        try:
            with open(tmp_filepath, 'w') as f:
                f.write(value + '\n' + md5digest)
        except Exception as e:
            log.exception(
                "%s: Failed to generate temp file %s for storing large size values"
                % (e.message, tmp_filepath))
            return (None, None)
        finally:
            f.close()
        transformed_command = command.replace(
            python_download_script, "%s -l %s" % (
                python_download_script, tmp_filepath))
        return transformed_command, tmp_filepath
    else:
        transformed_command = command.replace(
            python_download_script, "%s -v '%s'" % (
                python_download_script, value))
        return transformed_command, None


def _get_local_file_path_from_command(command):
    try:
        parts = command.split(' ')
        return parts[parts.index('-f') + 1]
    except:
        log.error("Error getting local file path for %s" % command)
        return None


def _get_and_set_local_file_modification_time(zk_path, command, watch_type):
    global _PATH_TO_DATA
    global _SERVERSET_METADATA
    local_file_path = _get_local_file_path_from_command(command)
    if not local_file_path:
        modification_timestamp = 0
    else:
        try:
            modification_timestamp = os.path.getmtime(local_file_path)
        except:
            modification_timestamp = 0
    if watch_type == 'serverset':
        _SERVERSET_METADATA[zk_path] = {
            'modification_timestamp': modification_timestamp
        }
    else:
        _PATH_TO_DATA[zk_path] = {
            'modification_timestamp': modification_timestamp
        }


##################################################
### Core Zookeeper Notification Handling Logic ###
##################################################
def _run_command(zk_path, command, max_wait_in_secs, watch_type, value=None, stat=None):
    """ Invoking the shell command
    """
    log.debug("Invoking command '%s'" % (command))
    global _PATH_TO_DATA
    data = _PATH_TO_DATA.get(zk_path, None)

    # ignore the update if the version is older than the current one
    if (data and stat and data.get("version", -1) >= stat.version):
        log.info(
            "Version %d is not newer than the current version %d, ignore"
            % (stat.version, data["version"])
        )
        return
    else:
        if stat:
            version = stat.version
        else:
            version = None
        notification_timestamp = time.time()
        log.info("Notification is queued for running command '%s' "
                 "with version %s and event timestamp %f"
                 % (command, str(version), notification_timestamp))
        _NOTIFICATION_EVENT_QUEUE.put(
            (zk_path, command, value, version,
             max_wait_in_secs, watch_type, notification_timestamp)
        )


def _notification_processor():
    while True:
        (zk_path, command, value, version, max_wait_in_secs,
         watch_type, notification_timestamp) = _NOTIFICATION_EVENT_QUEUE.get()

        if zk_path == "kill":
            _kill("Restart via kill api")

        # ignore all notifications with an older version
        if _is_older_version(zk_path, version, notification_timestamp):
            continue

        # TODO: we need to deal with it if the number of spawned greenlets
        # becomes an issue.
        gevent.spawn(_process_notification, command, value, version,
                     max_wait_in_secs, watch_type, zk_path,
                     notification_timestamp)


def _process_notification(command, value, version, max_wait_in_secs,
                          watch_type, zk_path, notification_timestamp):
    log.info("Processing notification event for command %s received at %f"
             % (command, notification_timestamp))
    log.info("ZkPath: %s, Version: %s, Time: %s" % (zk_path, str(version),
                                                    str(datetime.datetime.now())))

    # Record the timestamp that the execution is scheduled
    _COMMAND_TO_NOTIFICATION_TIME[command] = datetime.datetime.now()

    if max_wait_in_secs is not None and max_wait_in_secs > 0:
        wait_in_seconds = random.randint(0, max_wait_in_secs)
        log.info("Sleeping %d seconds" % (wait_in_seconds))
        gevent.sleep(wait_in_seconds)

    log.debug("Command %s is executed at %s"
              % (command, str(datetime.datetime.now())))

    # Right before executing the command, check the version to not overwrite
    # with an older version
    if _is_older_version(zk_path, version, notification_timestamp):
        return

    if value:
        # To reduce the load to zookeeper by not downloading value when the
        # value is already in notification, try to insert the value to the
        # command line as an argument of the download script.
        #
        # for now, we know that the rich data stored in the zknode (rather
        # than take from the path) is in a different format, so we disable
        # this optimization in the case of --inspect-node-data

        if watch_type == 'serverset':
            value = [str(server) for server in list(value)]
            value = list(set(value))
            value.sort()
            value = string.join(value, ',')
        log.info("Trying to pass value to the download command %s",
                 command)
        value = str(value).replace("'", "\\'")

        (command, tmp_filepath) = transform_command_with_value(
            command, value, notification_timestamp)
        if command is None:
            return

    if zk_path in _PATH_TO_DATA:
        _PATH_TO_DATA[zk_path].update({
            "version": version,
            "notification_timestamp": notification_timestamp
        })
    else:
        _PATH_TO_DATA[zk_path] = {
            "version": version,
            "notification_timestamp": notification_timestamp
        }
    log.debug("zk_path: %s value updated to value %s, version %s, timestamp %f"
              % (zk_path, value, version, notification_timestamp))

    # check if rate limit is triggered - if yes,we delay the process by putting
    # it into the holddown queue.
    if _check_download_interval(zk_path, time.time()):
        _run_command_and_check_ret_code(
            command, os.environ.copy(),
            _PATH_TO_ALERT_DISABLED.get(zk_path, False),
            watch_type=watch_type,
            zk_path=zk_path,
            notification_timestamp=notification_timestamp,
            value=value)
    else:
        log.warn("Rate limit for %s" % zk_path)
        _NOTIFICATION_HOLDDOWN_QUEUE.put(
            (zk_path, command, value, version,
             max_wait_in_secs, watch_type, notification_timestamp)
        )

    if value and len(value) > _LONG_VALUE_THRESHOLD and os.path.isfile(tmp_filepath):
        os.remove(tmp_filepath)


def _run_command_and_check_ret_code(command, env, alert_disabled,
                                    retry_count=0, watch_type=None,
                                    zk_path=None, notification_timestamp=None,
                                    value=None,
                                    downloader_report_metadadata=False):
    try:
        # There are two code paths for metadata report
        # (1) downloader_report_metadadata is True. It is set to true
        # when _refresh_all_commands() get called. When set to true,
        # zk_download_data reports metadata to the flask endpoint
        # /admin/report_metadata.
        # (2) downloader_report_metadadata is False. When a node change is
        # observed by watcher, only serverset metadata
        # is calculated and updated in zk_update_monitor itself.
        if downloader_report_metadadata:
            command += " --report-metadata"
        command += EXTRA_FLAGS_FOR_ZK_DOWNLOAD_DATA
        subprocess.check_call(command, shell=True, env=env)
        log.debug("Command %s is executed successfully." % command)
        if (not downloader_report_metadadata) \
                and watch_type == 'serverset' \
                and zk_path and notification_timestamp and value:
            update_serverset_metadata(zk_path, notification_timestamp, value)
        if zk_path in _REJECTED_UPDATED_SERVERSETS:
            _REJECTED_UPDATED_SERVERSETS.remove(zk_path)
    except subprocess.CalledProcessError as e:
        error_code = e.returncode
        if error_code in _IGNORED_RET_CODES:
            log.debug("Got return code '%d' for command '%s' and ignored"
                      % (error_code, command))
            if (not downloader_report_metadadata) \
                    and watch_type == 'serverset' \
                    and zk_path and notification_timestamp and value:
                update_serverset_metadata(zk_path, notification_timestamp, value)
        elif error_code in _RETRY_RET_CODES and retry_count < _MAX_RETRY_COUNT:
            retry_count += 1
            log.info("Retry command after %d seconds"
                     % (_RETRY_WAIT_TIME_IN_SECONDS))
            gevent.sleep(_RETRY_WAIT_TIME_IN_SECONDS)
            _run_command_and_check_ret_code(command, env,
                                            alert_disabled, retry_count,
                                            watch_type, zk_path,
                                            notification_timestamp, value)
        else:
            log.error("Got return code %d for command '%s'"
                      % (error_code, command))
            if error_code in _SERVERSET_REJECTION_ERROR_CODES and zk_path:
                if zk_path not in _REJECTED_UPDATED_SERVERSETS:
                    _REJECTED_UPDATED_SERVERSETS.add(zk_path)


#############################################
### Periodical Checking/Refreshing Logic ####
#############################################
def _periodic_checking():
    while True:
        try:
            log.info('sleeping... for %s seconds'
                     % (str(WAKEUP_FREQUENCY_IN_SECS)))
            gevent.sleep(float(WAKEUP_FREQUENCY_IN_SECS))
            # Wait up to MAX_JITTER_TIME_IN_SECS seconds before invoking to lower
            # the likelihood that many machines invoke do this at the same time
            gevent.sleep(random.randint(0, MAX_JITTER_TIME_IN_SECS))
            log.info('Executing all associated commands...')
            if random.uniform(0, 1) < RESTART_PROBABILITY:
                _kill("Restart from periodic_checking")
            _refresh_all_commands()
            _retry_nonexistent_zk_paths(list(_CONFIGS_WITH_NONEXISTENT_PATH))
        except Exception:
            log.exception("Periodic checking error")


def _refresh_all_commands():
    for zk_path, command in _PATH_TO_COMMAND.iteritems():
        try:
            if command in _COMMAND_TO_NOTIFICATION_TIME:
                now = datetime.datetime.now()
                previous_start = _COMMAND_TO_NOTIFICATION_TIME[command]
                log.debug("Command %s was last started at %s"
                          % (command, str(previous_start)))
                duration = now - previous_start
                if duration.seconds <= _CONCURRENT_MIN_WINDOW_IN_SECS:
                    log.info("It is not more %d seconds after command %s "
                             "was executed, ignore"
                             % (_CONCURRENT_MIN_WINDOW_IN_SECS, command))
                    continue

            log.info("It is time to execute the command %s for path %s"
                     % (command, zk_path))
            env = os.environ.copy()
            # Set a special variable to indicate this command invocation is
            # from a periodic refresh so the invoked command can probably
            # do some special handling.
            env['__ZK_UPDATE_MONITOR_REFRESH__'] = 'True'
            _run_command_and_check_ret_code(
                command, env, _PATH_TO_ALERT_DISABLED.get(zk_path, False),
                zk_path=zk_path, downloader_report_metadadata=True)
        except:
            log.exception("Refresh commands exception")


def _retry_nonexistent_zk_paths(configs):
    if configs:
        log.info("Retrying %d configs with non-existent watched zookeeper path"
                 % len(configs))
        failed = [x for x in configs if not _place_watch_from_metaconfig(x)]
        del _CONFIGS_WITH_NONEXISTENT_PATH[:]
        _CONFIGS_WITH_NONEXISTENT_PATH.extend(failed)


def _dump_command_processing_info():
    while True:
        gevent.sleep(_DUMP_COMMAND_PROCESSING_INFO_INTERVAL_IN_SECONDS)
        log.info("##### COMMAND PROCESSING INFO #####")
        paths = list(_PATH_TO_COMMAND.keys())
        paths.sort()
        for path in paths:
            command = _PATH_TO_COMMAND.get(path)
            log.info("%s: %s was executed at %s",
                     path,
                     str(command),
                     str(_COMMAND_TO_NOTIFICATION_TIME.get(command)))
        log.info("############## DONE ###############")


#################################
### Rate Limit Implementation ###
#################################
def _holddown_queue_wiper():
    # A greenlet which wakes up every X seconds to clean up
    # messages in the holddown queue. It either drops the event
    # or put the event back to the _NOTIFICATION_EVENT_QUEUE.
    while True:
        gevent.sleep(_NOTIFICATION_HOLDDOWN_WIPER_SLEEP_INTERVAL_IN_SECONDS)
        while not _NOTIFICATION_HOLDDOWN_QUEUE.empty():
            (zk_path, command, value, version, max_wait_in_secs,
             watch_type, notification_timestamp) \
                = _NOTIFICATION_HOLDDOWN_QUEUE.get()
            if (zk_path not in _PATH_TO_DATA) or notification_timestamp \
                    >= _PATH_TO_DATA[zk_path]['notification_timestamp']:
                _NOTIFICATION_EVENT_QUEUE.put(
                    (zk_path, command, value, version,
                     max_wait_in_secs, watch_type, notification_timestamp)
                )


def _serverset_type_set_wiper():
    """
    This is a greenlet which invalidated the cache of serverset_type
    every 30 min
    """
    global _SKIP_FLAPPY_CHECK_SERVERSETS
    global _NON_SKIP_FLAPPY_CHECK_SERVERSETS
    while True:
        gevent.sleep(_FLAPPY_CHECK_SERVERSETS_TTL)
        _SKIP_FLAPPY_CHECK_SERVERSETS = set()
        _NON_SKIP_FLAPPY_CHECK_SERVERSETS = set()


#########################################################
####### Funcs dealing with MetaConfig/Dependencies ######
#########################################################
def gevent_load_metaconfigs(dependency):
    try:
        log.info("Sleep 3 seconds for FLASK to start before actually loading")
        gevent.sleep(3)
        # We prioritize the bootstraping of configs from ConfigV3
        _load_metaconfigs_from_one_dependency(dependency)
        global _INITIALIZATION_COMPLETED
        _INITIALIZATION_COMPLETED = True
    except BaseException:
        _kill("Restart because initialization error")


def _load_metaconfigs_from_one_dependency(dependency_name):
    """ Load the metaconfigs from one dependency and finally from S3
    It does 3 things:
    1. Set children watch on dependencies. If the root dependency contains other
     dependencies, set watchers.
    2. Set znode watch on each MetaConfig Znode
    3. Load the MetaConfig content from S3 using S3Utils.

    If dependency/metaconfig does not exist, log and skip.
    """
    if dependency_name in _WATCHED_DEPENDENCIES:
        log.info("Dependency %s already watched" % dependency_name)
        return
    dependency_zk_path = DEPENDENCY_ZK_PATH_FORMAT.format(dependency_name)
    if not _kazoo_client(ZK_HOSTS).exists(dependency_zk_path):
        log.error("The dependency %s does not exist" % dependency_zk_path)
        return
    # Set ZK children watch on dependency
    log.info("Watching dependency %s" % dependency_name)
    _kazoo_client(ZK_HOSTS).ChildrenWatch(dependency_zk_path, update_dependency)
    _WATCHED_DEPENDENCIES.add(dependency_name)


def update_dependency(dependents):
    # For all children under dependency, set datawatch or children watches
    # If this function is triggered not in the placing watchers time,
    # restart ZUM to refresh to dependency list.
    if _INITIALIZATION_COMPLETED:
        _kill("Watched dependency changed, restart ZUM to catch the change")

    for dependent in dependents:
        if dependent.endswith(".dep"):
            _load_metaconfigs_from_one_dependency(dependent)
        else:
            if dependent in _WATCHED_METACONFIGS:
                continue
            # Set watch on the MetaConfig znode and load the content from S3
            metaconfig_zk_path = METACONFIG_ZK_PATH_FORMAT.format(dependent)
            if not _kazoo_client(ZK_HOSTS).exists(metaconfig_zk_path):
                log.error("The metaconfig %s does not exist" % dependent)
                continue
            log.info("Watching Metaconfig %s" % dependent)
            watcher = DataWatcher(metaconfig_zk_path, ZK_HOSTS)
            watcher.watch(functools.partial(update_metaconfig, dependent))
            _WATCHED_METACONFIGS.add(dependent)


def update_metaconfig(metaconfig_name, *args, **kwargs):
    zk_value, version = _kazoo_client(ZK_HOSTS).get(
        METACONFIG_ZK_PATH_FORMAT.format(metaconfig_name))
    s3_key = METACONFIG_S3_KEY_FORMAT.format(metaconfig_name)
    s3_path = zk_util.construct_s3_path(s3_key, zk_value)
    try:
        metaconfig_data = s3config.S3Config(AWS_KEY_FILE, S3_BUCKET, s3_endpoint=S3_ENDPOINT).get_config_string(s3_path)
    except ValueError as ve:
        log.error("Abort downloading from s3 key %s due to ValueError: %s" % (s3_path, ve))
        return
    except Exception as e:
        log.error("Abort downloading from s3 key %s due to unexpected s3 exception: %s" %
                  (s3_path, e))
        return

    metaconfig_list = json.loads(metaconfig_data)
    for metaconfig in metaconfig_list:
        _place_watch_from_metaconfig(metaconfig)


def _place_watch_from_metaconfig(metaconfig):
    zk_path = metaconfig.get('zk_path')
    command = metaconfig.get("command")
    max_wait_in_secs = int(metaconfig.get("max_wait_in_secs", 0))
    watch_type = metaconfig.get("type", "config").lower()

    alert_disabled = \
        (metaconfig.get("alert_disabled", "False").lower() == "true")
    return _place_watch(metaconfig, zk_path, command,
                        watch_type, max_wait_in_secs, alert_disabled)


def _place_watch(config, zk_path, command,
                 watch_type, max_wait_in_secs=0, alert_disabled=False):
    """
    Place the watch for Config or Serverset giving the information extracted
    from metaconfig.
    """

    # If the ZK path is already added, skip the following steps
    # in order to resolve conflicts.
    if zk_path in _PATH_TO_COMMAND:
        log.warn("Path %s has already been added to PATH_TO_COMMAND" % zk_path)
        return False

    if not _zk_path_exists(ZK_HOSTS, zk_path):
        log.error("zk_path %s does not exist in zk_hosts %s, no watch is set."
                  % (zk_path, ZK_HOSTS))
        # Save the configs that contain the nonexistent zk path for retry
        _CONFIGS_WITH_NONEXISTENT_PATH.append(config)
        return False

    log.info("Creating Zookeeper data watcher for path %s of zk_hosts %s"
             % (zk_path, ZK_HOSTS))

    # Get the local file modification time if the file exists, else set to 0.
    _get_and_set_local_file_modification_time(zk_path, command, watch_type)

    if watch_type is not None and watch_type.lower() == 'serverset':
        watcher = ServerSet(zk_path, ZK_HOSTS)
    else:
        watcher = DataWatcher(zk_path, ZK_HOSTS)
    log.info(
        "Associating command '%s' with watcher of path %s"
        % (command, zk_path)
    )
    _PATH_TO_COMMAND[zk_path] = command
    _PATH_TO_ALERT_DISABLED[zk_path] = alert_disabled

    if watch_type == 'serverset':
        watcher.monitor(functools.partial(
            _run_command, zk_path, command, max_wait_in_secs, watch_type))
    else:
        watcher.watch(functools.partial(
            _run_command, zk_path, command, max_wait_in_secs, watch_type))

    return True


def initialize_zookeeper_aws_s3_configs(args):
    global ZK_HOSTS
    global ZK_HOSTS_FILE
    global AWS_KEY_FILE
    global S3_BUCKET
    global S3_ENDPOINT
    global EXTRA_FLAGS_FOR_ZK_DOWNLOAD_DATA

    ZK_HOSTS = zk_util.parse_zk_hosts_file(args.zk_hosts_file)
    ZK_HOSTS_FILE = args.zk_hosts_file
    AWS_KEY_FILE = args.aws_key_file
    S3_ENDPOINT = args.s3_endpoint
    S3_BUCKET = args.s3_bucket

    EXTRA_FLAGS_FOR_ZK_DOWNLOAD_DATA = " -z {} -a {} -b {} -e {} -o {}".format(
        ZK_HOSTS_FILE, AWS_KEY_FILE, S3_BUCKET, S3_ENDPOINT, args.port)


def main():
    global args
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-d", "--dependency", dest="dependency", metavar="DEPENDENCY",
        required=True,
        help="The root dependency name, which stores what "
             "config/serversets are needed to be watched by ZUM. "
             "The dependency can have either config/serverset name, "
             "or other dependencies."
    )
    parser.add_argument(
        "-p", "--port", dest="port", metavar="PORT",
        required=True,
        help="The port Flask app will be running on."
    )
    parser.add_argument(
        "-z", "--zk-hosts-file-path", dest="zk_hosts_file", metavar="ZKHOSTS",
        required=True,
        help="The path of file which have a list of Zookeeper endpoints "
             "(host:port) which keeps the metaconfig as well as "
             "the config/serversets"
    )
    parser.add_argument(
        "-a", "--aws-key-file", dest="aws_key_file", metavar="AWSKEY",
        required=True,
        help="The path of the file storing AWS access and secret keys",
    )
    parser.add_argument(
        "-b", "--s3-bucket", dest="s3_bucket", metavar="BUCKET",
        required=True,
        help="The S3 bucket storing metaconfigs / configs"
    )
    parser.add_argument(
        "-e", "--aws-s3-endpoint", dest="s3_endpoint", metavar="ENDPOINT",
        default="s3.amazonaws.com",
        help="The S3 endpoint storing metaconfig / configs"
    )

    args = parser.parse_args()
    initialize_zookeeper_aws_s3_configs(args)

    notification_processor = gevent.spawn(_notification_processor)
    dump_command_processing_info = gevent.spawn(_dump_command_processing_info)
    dependency = args.dependency
    gevent.spawn(gevent_load_metaconfigs, dependency)

    periodic_checking = gevent.spawn(_periodic_checking)
    holddown_queue_wiper = gevent.spawn(_holddown_queue_wiper)
    serverset_type_set_wiper = gevent.spawn(_serverset_type_set_wiper)
    _GREENLET_DICT["notification_processor"] = notification_processor
    _GREENLET_DICT["dump_command_processing_info"] = dump_command_processing_info
    _GREENLET_DICT["periodic_checking"] = periodic_checking
    _GREENLET_DICT["holddown_queue_wiper"] = holddown_queue_wiper
    _GREENLET_DICT["serverset_type_set_wiper"] = serverset_type_set_wiper

    log.info('starting a Flask WSGI server for stats reporting')
    pywsgi.WSGIServer(("0.0.0.0", int(args.port)), app).serve_forever()


######################################################################
### Various Flask Endpoints for Monitoring ZUM and get insights :) ###
######################################################################

@app.route("/admin/ping")
def ping():
    return "Ping!\n"


@app.route("/admin/kill")
def restart():
    """ Kill the running of ZK update monitor and restart
    """
    log.info("Killed via flask API /admin/kill")
    # Enqueue a kill event to the queue
    _NOTIFICATION_EVENT_QUEUE.put(
        ("kill", None, None, None, None, None, None))
    return ""


@app.route("/admin/greenlet")
def greenlet_check():
    status_dict = {name: not _GREENLET_DICT[name].dead for name in _GREENLET_DICT}
    return json.dumps(status_dict)


def _update_modification_timestamp_for_flask(mode, zk_path, modification_timestamp):
    current_modification_timestamp = _PATH_TO_DATA[zk_path].get(
        'modification_timestamp', 0) if mode == 'CONFIG' \
        else _SERVERSET_METADATA[zk_path].get('modification_timestamp', 0)
    if current_modification_timestamp > modification_timestamp:
        return
    if mode == 'SERVERSET':
        _SERVERSET_METADATA[zk_path].update(
            {'modification_timestamp': modification_timestamp})
    else:
        _PATH_TO_DATA[zk_path].update(
            {'modification_timestamp': modification_timestamp})


@app.route("/admin/report_metadata", methods=['POST'])
def report_metadata():
    """ Internal endpoint for zk_download_data to report metadata
    """
    zk_path = str(request.form.getlist('zk_path')[0])
    mode = str(request.form.getlist('mode')[0])
    notification_timestamp = float(request.form.getlist('notification_timestamp')[0])
    modification_timestamp = float(request.form.getlist('modification_timestamp')[0])

    if mode == 'SERVERSET':
        hashsum = int(request.form.getlist('hashsum')[0])
        num_endpoints = int(request.form.getlist('num_endpoints')[0])
        try:
            _SERVERSET_METADATA[zk_path].update({"hashsum": hashsum,
                                                "num_endpoints": num_endpoints,
                                                "notification_timestamp": notification_timestamp})
        except:
            log.error("Serverset {} is not loaded into dict but metadata is being reported".format(zk_path))
    else:
        version = int(request.form.getlist('version')[0])
        try:
            _PATH_TO_DATA[zk_path].update({"version": version,
                                           "notification_timestamp": notification_timestamp})
        except:
            log.error("Config {} is not loaded into dict but metadata is being reported".format(zk_path))

    _update_modification_timestamp_for_flask(mode, zk_path, modification_timestamp)
    return ""


@app.route("/admin/report_file_modification", methods=['POST'])
def report_file_modification():
    """ When zk_download_data actual write to the local file,
    it reports the time to this endpoint. This is the actual time
    when file get changed.
    """
    zk_path = str(request.form.getlist('zk_path')[0])
    mode = str(request.form.getlist('mode')[0])
    modification_timestamp = float(request.form.getlist('modification_timestamp')[0])
    _update_modification_timestamp_for_flask(mode, zk_path, modification_timestamp)
    return ""


@app.route("/config")
def flask_get_configs():
    """_PATH_TO_DATA also contains serverset paths,
    filter them out so only configs are exported here,
    to get data please use /serverset/ endpoint
    """
    filtered_config_metadata = {
        filtered_zk_path: _PATH_TO_DATA[filtered_zk_path]
        for filtered_zk_path in _PATH_TO_DATA.keys()
        if not filtered_zk_path.startswith('/discovery')
    }
    return json.dumps(filtered_config_metadata)


@app.route("/config/<path:config_path>")
def flask_get_config(config_path):
    return json.dumps(_PATH_TO_DATA.get('/' + config_path))

@app.route("/serverset")
def flask_get_serversets():
    return json.dumps(_SERVERSET_METADATA)


@app.route("/serverset/<path:serverset_path>")
def flask_get_serverset(serverset_path):
    return json.dumps(_SERVERSET_METADATA.get('/' + serverset_path))


@app.route("/admin/rejected_serversets")
def get_rejected_serversets():
    return json.dumps(list(_REJECTED_UPDATED_SERVERSETS))


@app.route("/admin/rejected_serversets/healthcheck")
def rejected_serversets_healthcheck():
    if not _REJECTED_UPDATED_SERVERSETS:
        message = {"status": 200}
        resp = jsonify(message)
        resp.status_code = 200
    else:
        message = {"status": 500}
        resp = jsonify(message)
        resp.status_code = 500
    return resp


# Endpoints for nagios/saigon healthcheck, see whether zk_updater is up and running
# Return 200 if all greenlet is running well.
# Return 204 if greenlets are well, but intiilization is not completed.
# Return 500 if not all greenlets are working.
@app.route("/admin/healthcheck")
def flask_healthcheck():
    healthy = True
    for greenlet_name in _GREENLET_DICT:
        if _GREENLET_DICT[greenlet_name].dead:
            healthy = False
            break
    if not healthy:
        message = {"status": 500}
        resp = jsonify(message)
        resp.status_code = 500
    else:
        if not _INITIALIZATION_COMPLETED:
            message = {"status": 204}
            resp = jsonify(message)
            resp.status_code = 204
        else:
            message = {"status": 200}
            resp = jsonify(message)
            resp.status_code = 200
    return resp


# Endpoint for recording _SKIP_FLAPPY_CHECK_SERVERSETS
# and _NON_SKIP_FLAPPY_CHECK_SERVERSETS
@app.route("/admin/report_serverset_type", methods=['POST'])
def report_serverset_type():
    zk_path = str(request.form.getlist('zk_path')[0])
    mode = str(request.form.getlist('mode')[0])
    if mode == "0":
        _SKIP_FLAPPY_CHECK_SERVERSETS.add(zk_path)
    else:
        _NON_SKIP_FLAPPY_CHECK_SERVERSETS.add(zk_path)
    return ""


# Endpoint for checking if a serverset is in _SKIP_FLAPPY_CHECK_SERVERSETS or
# _NON_SKIP_FLAPPY_CHECK_SERVERSETS or doesn't exist in either.
@app.route("/admin/query_serverset_type", methods=['POST'])
def query_serverset_type():
    zk_path = str(request.form.getlist('zk_path')[0])
    if zk_path in _SKIP_FLAPPY_CHECK_SERVERSETS:
        return "0"
    elif zk_path in _NON_SKIP_FLAPPY_CHECK_SERVERSETS:
        return "1"
    else:
        return "-1"


@app.route("/admin/query_non_skip_check_flappy_serversets")
def query_non_skip_check_flappy_serversets():
    return json.dumps(list(_NON_SKIP_FLAPPY_CHECK_SERVERSETS))


@app.route("/admin/query_skip_check_flappy_serversets")
def query_skip_check_flappy_serversets():
    return json.dumps(list(_SKIP_FLAPPY_CHECK_SERVERSETS))


@app.route("/admin/watched_metaconfigs")
def query_watched_metaconfigs():
    return json.dumps(list(_WATCHED_METACONFIGS))


@app.route("/admin/watched_dependencies")
def query_watched_dependencies():
    return json.dumps(list(_WATCHED_DEPENDENCIES))


# The following variables and apis are only used for unit testing.
test_notification_count = 0


@app.route("/admin/report_unittest_notification")
def report_unittest_notification():
    global test_notification_count
    test_notification_count += 1
    return ""


if __name__ == "__main__":
    initialize_logger(logger_filename="zk_update_monitor.log")
    main()
