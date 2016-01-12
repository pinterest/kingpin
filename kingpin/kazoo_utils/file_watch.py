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


"""Utilities for monitoring local file changes gevent.

This module contains a FileWatch class, which can be used to monitor a set
of local files. One or more callback functions can be registered for a file
path. When there is modification to the file's content and change of last
modified time, the registered callback functions for the file will be invoked.
Update detection currently is implemented by periodic polling.

For the case that file is not existent, there are 2 cases:
(1) if keep_retrying is False, the add_watch() will through OSError exception
(2) if keep_retrying is True, the greenlet will keep trying to open the file
from the file_path in an exponential backoff way.
File deletion is not triggering the invocation of the callback functions as well.
For a file that was monitored, deleted, and then brought back, the callback functions
would be called at the time when it was back.

There are two types of watches, config watch and serverset watch. The main
difference is the callback function's type signature. The callback of
config watches takes the file content and a stat object which has a version
attribute. The callback of serverset only takes the file content as input.
Having these two types mainly is for being compatible with the watches
provided in the open source kazoo library. The same file path can have both
types of watches at the same time.

The FileWatch class is a Singleton so there is only going to be one greenlet
polling file changes periodically.

Adding a config watch is simply as below::

    def on_change(value, stat):
        ...

    FileWatch().add_watch(file_path, on_change)

And the following code adds a serverset watch::

    def on_change(servers):
        ...

    FileWatch().add_watch(file_path, on_change, watch_type='serverset')


This class is expected to have the same behavior as the ConfigFileWatcher
Java class in Pinterest/optimus. Please make sure you change the Java class
as well when you make change here.

"""
from __future__ import absolute_import

from collections import namedtuple
import datetime
from functools import wraps
import gevent
import hashlib
import logging
import os

from .utils import dummy_statsd
from .utils import hostname
from .utils import _escape_path_for_stats_name
from .decorators import SingletonMetaclass
from ..thrift_utils.periodical import Periodical

log = logging.getLogger(__name__)


# Currently the polling is by default to run every 30 seconds.
# The class is a singleton, we can come up a way to allow customizing
# the frequency in the future if necessary.
_POLLING_WAIT_IN_SECONDS = 30


def timeout_after(secs):
    """Decorator to timeout a function.

    It raises a gevent.Timeout exception after the specified seconds in
    the decorated function. The timeout will work only if the decorated
    function yields, e.g. performing blocking operations through gevent.

    """
    def timeout_enforced(f):
        @wraps(f)
        def g(*args, **kwargs):
            return gevent.with_timeout(secs, f, *args, **kwargs)
        return g
    return timeout_enforced


class FileWatch:

    __metaclass__ = SingletonMetaclass

    def __init__(self, polling=True,
                 polling_wait_in_seconds=_POLLING_WAIT_IN_SECONDS,
                 sc=dummy_statsd):
        """
        Args:
            polling: flag to indicate whether the greenlet for polling
             file changes is started, having this option is mainly for unit
             testing so that tests can disable polling.
             polling_wait_in_seconds: Mostly for testing, allow to inject a small
             wait time so tests can go through quickly.
        """
        # a map from (file_path, watch_type) to (timestamp, hash, [func])
        self._watched_file_map = {}
        self._sc = sc
        if polling:
            Periodical(
                'file_watch_polling',
                polling_wait_in_seconds,
                polling_wait_in_seconds,
                self._check_file_updates)

        # To simulate the behavior from kazoo DataWatch/ServerSet, result
        # type needs to have version attribute
        self.Stat = namedtuple('Stat', ['version'])

    def _calculate_backoff_time(self, num_tries, backoff_in_secs, max_wait_in_secs):
        return min((2**num_tries) * backoff_in_secs, max_wait_in_secs)

    def add_watch(self, file_path, on_change, watch_type='config',
                  keep_retrying=False, backoff_in_secs=10,
                  max_wait_in_secs=300):
        """Adds a watch for an existing path.

        Args:
            file_path: Must be an existent file, otherwise raises
            on_change: A callback function on file change
            watch_type: The watch type parameter can be 'config' or
                'serverset'.
            keep_retrying: Whether to keep retrying to add this file watch
                in case of failure. By default it is ``False``.
            backoff_in_secs: how much to backoff between retries. retry
                backoff will be exponentially backoff.
            max_wait_in_secs: max wait in seconds between retries.

        """
        num_tries = 0
        assert file_path
        assert on_change
        self._validate_watch_type(watch_type)

        if not keep_retrying:
            # If not keep retrying, this is a synchronous call,
            # if fails then it fails.
            last_update_time = os.path.getmtime(file_path)
        else:
            # Running the background greenlet, keep retrying until the
            # serverset file is there.
            while True:
                if os.path.isfile(file_path):
                    last_update_time = os.path.getmtime(file_path)
                    break
                else:
                    log.exception("non existing file of type %s, on %s" % (
                        watch_type, file_path))
                    # exponential backoff
                    gevent.sleep(self._calculate_backoff_time(
                        num_tries, backoff_in_secs, max_wait_in_secs))
                    num_tries += 1

        num_tries = 0
        file_path_stat_name = _escape_path_for_stats_name(file_path)

        log.info("try to add file watch of type %s, on %s" % (
            watch_type, file_path))
        while keep_retrying or num_tries == 0:
            try:
                # Read the file and make the initial onUpdate call.
                with gevent.Timeout(seconds=30):
                    with open(file_path, 'r') as f:
                        file_content = f.read()
                    try:
                        self._invoke_callback(
                            on_change, watch_type, file_content,
                            self.Stat(version=last_update_time))
                    except Exception:
                        log.exception("Exception in watcher callback for %s, "
                                      "ignoring" % file_path)
                    log.debug('The last modified timestamp of file %s is %f'
                              % (file_path, last_update_time))

                md5_hash = self._compute_md5_hash(file_content)
                log.debug('The md5 hash of file %s is %s' % (
                    file_path, md5_hash))

                key = (file_path, watch_type)
                if key in self._watched_file_map:
                    # Append on_change to the file_path's existing callbacks
                    self._watched_file_map[key][2].append(on_change)
                else:
                    self._watched_file_map[key] = (
                        last_update_time, md5_hash, [on_change])
                log.info("successfully added file watch of type %s, on %s" % (
                    watch_type, file_path))
                break
            except Exception, gevent.Timeout:
                log.exception("failed to add file watch of type %s, on %s" % (
                    watch_type, file_path))
                self._sc.increment(
                    "errors.file.{}watch.failure.{}.{}".format(
                        watch_type, file_path_stat_name, hostname),
                    sample_rate=1)
                if keep_retrying:
                    # exponential backoff
                    gevent.sleep(self._calculate_backoff_time(
                        num_tries, backoff_in_secs, max_wait_in_secs))
                    num_tries += 1
                else:
                    raise

    @timeout_after(30)
    def _check_file_updates(self):
        start_time = datetime.datetime.now()
        for key, value in self._watched_file_map.iteritems():
            (file_path, watch_type) = key
            (update_time, hash, callbacks) = value
            try:
                new_update_time = os.path.getmtime(file_path)
                if new_update_time != update_time:
                    with open(file_path, 'r') as f:
                        file_content = f.read()
                    new_hash = self._compute_md5_hash(file_content)
                    if new_hash != hash:
                        self._sc.gauge('file_watch.{}'.format(_escape_path_for_stats_name(file_path)),
                                       new_update_time,
                                       tags={'host': hostname})
                        log.info("File %s was modified at %f, notifying watchers."
                                 % (file_path, new_update_time))
                        for callback in callbacks:
                            try:
                                self._invoke_callback(
                                    callback,
                                    watch_type,
                                    file_content,
                                    self.Stat(version=new_update_time))
                            except:
                                log.exception("Exception in watcher callback for %s, ignoring" % file_path)
                    else:
                        log.info("File %s was modified at %f but content hash is unchanged."
                                 % (file_path, new_update_time))
                    self._watched_file_map[key] = (new_update_time, new_hash, callbacks)
                else:
                    log.debug("File %s is not changed since %f"
                              % (file_path, update_time))
            except:
                # We catch and log exceptions related to the update of any specific
                # file, but move on so others aren't affected. Issues can happen for
                # example if the watcher races with an external file replace
                # operation, in which case the next run should pick up the update.
                log.exception("Config update check failed for %s" % file_path)
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        log.debug("Checking updates takes %d microseconds" % duration.microseconds)

    def _clear_all_watches(self):
        """
        Clears all watches, only used for testing.
        """
        self._watched_file_map.clear()

    def _invoke_callback(self, callback, watch_type, value=None, stat=None):
        self._validate_watch_type(watch_type)

        if watch_type == 'config':
            # apply the callback to value and stat
            callback(value, stat)
        elif watch_type == 'serverset':
            # apply the callback to a list of hosts
            if not value:
                list = []
            else:
                list = value.split('\n')
                # Remove empty strings
                list = filter(lambda s: s, list)
            callback(list)

    def _validate_watch_type(self, watch_type):
        if watch_type not in ['config', 'serverset']:
            raise Exception("Unrecognized watch type: %s" % watch_type)

    def _compute_md5_hash(self, s):
        assert s is not None
        m = hashlib.md5()
        m.update(s.encode('utf-8'))
        return m.hexdigest()
