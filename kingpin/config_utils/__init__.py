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

import logging
import os
import time

from kingpin.kazoo_utils import DataWatcher
from kingpin.kazoo_utils import KazooClientManager
import s3config

log = logging.getLogger(__name__)

CONFIG_FILE_PATH_FORMAT = '/var/config/%s'

S3_CONFIG_FILE_PATH_FORMAT = '/data%s'  # based upon zk_path.
ZK_LOCK_PATH_FORMAT = '%s.lock'  # based upon zk_path.


def get_config_file_path(zk_path, config_file_path_format=CONFIG_FILE_PATH_FORMAT):
    """Get config file path based upon zk path.

    e.g. '/config/manageddata/spam/domain_hidelist' ->
         '/var/config/config.manageddata.spam.domain_hidelist'

    """
    return config_file_path_format % zk_path[1:].replace('/', '.')


def get_zk_path(config_file_path):
    """Get zk path based upon config file path.

    e.g. '/var/config/config.manageddata.spam.domain_hidelist' ->
         '/config/manageddata/spam/domain_hidelist'

    """
    return '/' + config_file_path.split('/')[-1].replace('.', '/')


class ZKBaseConfigManager(object):
    """ A manager which pushes data to S3 and update the corresponding Zookeeper node.
    The manager doesn't take care of local copy of the config and the file watcher related
    work. The local file / file watcher logic is implemented in its subclass ZKConfigManager.

    """
    def __init__(self, zk_hosts, zk_path, aws_keyfile, s3_bucket, s3_endpoint="s3.amazonaws.com"):
        """
        ZKBaseConfigManager accepts a zk_path and will convert it to S3 path to do the double
            zk_path: the zookeeper path of the config/ serverset
        """
        self.zk_hosts = zk_hosts
        self.zk_path = zk_path
        self.aws_keyfile = aws_keyfile
        self.s3_bucket = s3_bucket
        self.s3_endpoint = s3_endpoint
        self.zk_lock_path = ZK_LOCK_PATH_FORMAT % self.zk_path
        self.s3_file_path = S3_CONFIG_FILE_PATH_FORMAT % self.zk_path

    def _get_s3_path_with_timestamp(self, timestamp):
        return "%s_%s" % (self.s3_file_path, timestamp)

    def get_data(self):
        """Get the version(timestamp) from zk and read the data from S3 and return."""
        KazooClientManager(self.zk_hosts).get_client().ensure_path(self.zk_path)
        # znode_data stores the timestamp used to determine the s3 path.
        znode_data, znode = KazooClientManager(self.zk_hosts).get_client().get(self.zk_path)

        s3_path = self._get_s3_path_with_timestamp(znode_data)
        return s3config.S3Config(self.aws_keyfile, self.s3_bucket, self.s3_endpoint).get_config_string(s3_path)

    def _get_data_without_zk(self):
        """Get the version from S3 and read the data from S3 and return.

        Note: get_data() is the recommended API to get the config data. This API should only be
        used when zk is down.

        """
        timestamp_read_from_s3 = s3config.S3Config(self.aws_keyfile, self.s3_bucket, self.s3_endpoint).get_config_string(
            self.s3_file_path)
        s3_path = self._get_s3_path_with_timestamp(timestamp_read_from_s3)
        return s3config.S3Config(self.aws_keyfile, self.s3_bucket, self.s3_endpoint).get_config_string(s3_path)

    def update_zk(self, old_value, value, force_update=True):
        """Update the s3 file and update the zk_node.

        All the operations inside this function is guarded by a distributed zk lock. It prevents
        race condition where multiple calls to try to update the config.
        Inside the lock, the given ``old_value`` is checked against the value in s3(located by zk
        node value) by default. Abort if they do not match unless force_update is True. Then the
        new data is uploaded to s3 whose key is suffixed with the current timestamp. Finally the
        zk node is updated with the current timestamp(which triggers zk_update_monitor to download).
        The last two steps cannot be reversed because we can only trigger zk_update_monitor to
        download when the new data is already in s3.

        If enable_audit_history is True, and author and comment are both given,
        we will log this change to audit history.

        Args:
            old_value: A string, which should be equal to the current value in zk.
                             old_value will be ignored if force_update is True
            value: A string, value to update to.
            force_update: Boolean, force update zk regardless if old_value matches s3_value or not. Default to be True.
        Returns:
            True if successfully updated, otherwise False.

        """
        # Avoid potential kazoo client problem.
        if not KazooClientManager(self.zk_hosts).get_client():
            KazooClientManager(self.zk_hosts)._reconnect()

        KazooClientManager(self.zk_hosts).get_client().ensure_path(self.zk_path)

        # Try to get the lock.
        lock = KazooClientManager(self.zk_hosts).get_client().Lock(self.zk_lock_path)
        if not lock.acquire(blocking=False):
            raise Exception('ZK lock is hold by someone else. Try later.')

        try:
            znode_data, znode = KazooClientManager(self.zk_hosts).get_client().get(self.zk_path)
            # Only allow update if the given old value is the current s3 value, or the value of the
            # zk_node is empty(which is the case where the zk node is updated for the first time).
            if not force_update and znode_data:
                s3_path_with_timestamp = self._get_s3_path_with_timestamp(znode_data)
                try:
                    s3_value = s3config.S3Config(self.aws_keyfile, self.s3_bucket, self.s3_endpoint).get_config_string(
                        s3_path_with_timestamp)
                except ValueError as e:
                    log.error("Failed to get s3 value from s3 path %s: %s" %
                              (s3_path_with_timestamp, str(e)))
                    raise Exception('Old s3 key %s located by the zk node value does not exist: '
                                    '%s This is possibly due to s3 inconsistency. Try later.' %
                                    (s3_path_with_timestamp, str(e)))

                if old_value != s3_value:
                    raise Exception('Old value is not equal to s3 value for zk path %s, old_value: %s, s3_value: %s' %
                                    (self.zk_path, old_value, s3_value))

            update_time = time.time()
            current_timestamp_str = str(update_time)
            s3_path_with_timestamp = self._get_s3_path_with_timestamp(current_timestamp_str)

            result = s3config.S3Config(self.aws_keyfile, self.s3_bucket, self.s3_endpoint).put_config_string(s3_path_with_timestamp, value)
            if result is not None:
                raise Exception('Error writing to s3 path %s for zk path %s: %s' % (
                    s3_path_with_timestamp, self.zk_path, result))

            # Write the index also to S3(this will be the same data stored in zk). It is used to
            # provide easy access to the S3 data in case zk is down.
            s3config.S3Config(self.aws_keyfile, self.s3_bucket, self.s3_endpoint).put_config_string(
                self.s3_file_path, current_timestamp_str)

            # Try 10 times in case the write to zk failed. We want to make sure zk is changed because
            # s3 is already overwritten. Otherwise, there will be inconsistency between s3 file and
            # local config file.
            for i in xrange(0, 10):
                try:
                    KazooClientManager(self.zk_hosts).get_client().set(
                        self.zk_path, current_timestamp_str)
                    return True
                except Exception as e:
                    print e
                    log.info('Zk write failed for zk path %s with %s for the %d time' % (
                        self.zk_path, e, i + 1))
                    KazooClientManager(self.zk_hosts)._reconnect()
            raise Exception('Failed to write to zk path %s even though we already wrote to s3.' % (
                self.zk_path))
        finally:
            lock.release()


class ZKConfigManager(ZKBaseConfigManager):
    """An extention of ZKBaseConfigManager that provides watcher for local config file.

    Internally, the config file is stored in s3, which is downloaded to local config file by
    zk_update_monitor process(when it gets notified by zk). To change the config file, we need to
    push the data to s3 and update zk_node, which triggers zk_update_monitor to download the file
    on each box.

    Before using this class, puppet change needs to be made to let zk_update_monitor download your
    file. To make zk_path and config file path consistent, we use ``get_zk_path()`` and
    ``get_config_file_path()`` to do the mutual conversion. The zk node created and the config file
    to watch needs to have the consistent path.

    If you only intend to use ``ZKConfigManager`` to get notified when config file is changed, you
    can ignore ``update_zk()``. ``update_zk()`` provides a convenient way to change the original
    data for the config file.

    Usage:
        def read_new_config(new_config):
            print "New config is here", new_config

        watcher = ZKConfigManager('/var/config/test_config', read_new_config)

        # ``read_new_config()`` is called during watcher initialization, and from now on, if config
        # file is changed, ``read_new_config()`` is called with the new config.

    """
    def __init__(self, zk_hosts, aws_keyfile, s3_bucket, file_path, read_config_callback, s3_endpoint="s3.amazonaws.com", zk_path_suffix=''):
        """
        Args:
            file_path: config file path to watch.
            read_config_callback: callback function when new config is detected. It should take
                one argument which is the new config string. If it is None, no config file watcher
                will be registered.
        """

        self.zk_original_path = get_zk_path(file_path)
        self.zk_path = self.zk_original_path + zk_path_suffix

        super(ZKConfigManager, self).__init__(zk_hosts, self.zk_path, aws_keyfile, s3_bucket, s3_endpoint=s3_endpoint)

        self.file_path = file_path
        self.zk_lock_path = ZK_LOCK_PATH_FORMAT % self.zk_path
        self.s3_file_path = S3_CONFIG_FILE_PATH_FORMAT % self.zk_original_path

        if read_config_callback is not None:
            assert hasattr(read_config_callback, '__call__')
        self.read_config_callback = read_config_callback
        self.version = -1

        self.watcher = DataWatcher(None, zk_hosts, file_path=self.file_path)
        if file_path:
            try:
                os.path.getmtime(file_path)
            except OSError:
                log.error("%s does not exist or is unaccessible" % file_path)
                return

            self.watcher.watch(self._read_config)

    def _read_config(self, value, stat):
        """Callback function when config file is changed.

        It checks the current and new version to decide whether to call the caller's callback
        function.

        """
        new_version = 0 if stat is None else stat.version
        if self.version >= new_version:
            return
        if self.read_config_callback:
            self.read_config_callback(value)
        self.version = new_version

    def reload_config_data(self):
        """Get the data from config file and invoke callback function.

        Raises:
            ``IOError`` if the config file does not exist or accessible.

        """
        value, stat = self.watcher.get_data()
        self._read_config(value, stat)
