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

"""
Utilities for using gevent based kazoo zk client.

This module contains a KazooClientManager, which provides a KazooClient,
a high level zookeeper client; it also contains a ServerSet implementation,
which can be used to participate a server set or monitor the changes of a
server set.

To join a server set::

    server_set = ServerSet("/.../path_to_root_znode", file_containing_zk_endpoints)
    server_set.join("127.0.0.1:8080")

The sample code above will try to join the server set in the background,
and keep retrying in case of zookeeper client having difficulties in
communicating with zookeeper, once the connection with zookeeper is
established again, the endpoint will join the server set again. However,
if you want to try to join a server set, and synchronously checking on
whether you've succeeded, and based on that applying some custom logic,
you can do the following instead::

    try:
        g = server_set.join("127.0.0.1:8080", keep_retrying=False)
        g.get()  # if it fails, an exception will be thrown here
    except Exception:
        ...

Note that in the code above, keep_retrying is passed in as False,
which means joining server set will only be tried once, if it succeeds,
then sever set will keep maintaining the endpoint in the server set,
but if it fails, the exception will be raised to containing greenlet,
the caller can choose to handle it in desired way; and joining the server
set won't be maintained by the server set in this case.

To monitor a server set, a callback function that can expect a generator of
all endpoints in a server set needs to be provided, and then the caller can
simply do the following::

    def on_change(endpoints):
        for endpoint in endpoints:
            ...
        ...

    server_set.monitor(on_change)

Similarly the caller can also choose to try to place the monitor only once::

    try:
        g = server_set.monitor(on_change, keep_retrying=False)
        g.get()
    except Exception:
        ...  # for example, using the default list of endpoints
        # as an example, now have server set keeps retrying for me now
        server_set.monitor(on_change)

As for Datawatcher, one can use it in a similar way as monitoring serverset,
except that the callback should expect two parameters, one is the value of the
znode, the other is the stats of the znode. The stat parameter has the
version number of the correspondingn znode, which can be used in the
callback to discard out-of-order notification, should that ever happen.

For example::

    def on_change(value, stat):
        ...

    watcher = DataWatcher("/.../path_to_znode")
    watcher.watch(on_change)

If you want to do it synchronously, you can do the following instead::

    try:
        g = watcher.watch(on_change, keep_retrying=False)
        g.get()
    except Exception:
       ...  # Deal with the exception.

"""
from collections import namedtuple
import functools
import logging
import os
import random
import sys
import socket
import time

import gevent
from decorators import SingletonMetaclass
from kazoo.client import KazooClient
from kazoo.handlers.gevent import SequentialGeventHandler
from kazoo.protocol.states import KazooState
from utils import dummy_statsd
from utils import hostname
from utils import _escape_path_for_stats_name
from file_watch import FileWatch

log = logging.getLogger(__name__)


class NoClientException(Exception):
    """Special exception when the underlying client is ``None``."""
    pass


class KazooClientManager(object):
    """Singleton manager for maintaining a healthy underlying zk client."""

    __metaclass__ = SingletonMetaclass

    MIN_HEALTH_LOG_INTERVAL_IN_SECS = 60

    def __init__(self, zk_hosts, max_num_consecutive_failures=3,
                 health_check_interval=30, sc=dummy_statsd, start_timeout=25.0,
                 session_timeout=16.0):

        """Constructor.

        Args:
            zk_hosts: a list of host:port. For example,
                the list should have the following format:
                ['observerzookeeper010:2181',
                 'observerzookeeper011:2181',
                 'observerzookeeper012:2181',
                 'observerzookeeper013:2181',
                 'observerzookeeper014:2181',
                 'observerzookeeper015:2181']
            max_num_consecutive_failures: the max number of consecutive
                failures can be tolerated by the buddy health checking
                greenlet before it attempts to recreate a new connection.
            max_health_check_interval: the number of seconds between health
                checks.
            sc: stats_client. See example implementation in utils.py. You can
                plug the stats_client with any stats backend, for example tsd.
            start_timeout: the start timeout (in float) of kazoo client
                connecting to Zookeeper
            session_timeout: a timeout (in float) for zookeeper sessions.

        """
        # The list to keep track of the callbacks to be invoked when client
        # has to be torn down and recreated.
        self.zk_hosts = zk_hosts
        self.start_timeout = start_timeout
        self.session_timeout = session_timeout
        self._client_callbacks = []
        self._max_num_consecutive_failures = max_num_consecutive_failures
        self._health_check_interval = health_check_interval
        self._last_success_health_check_ts = time.time()
        self._last_healthy_log_ts = None
        self._is_destroyed = False
        self._sc = sc

        # Trying to connect to zookeeper.
        self._start("Failed to connect to zk.", True)

    def is_current_client(self, client):
        """Check whether the client held by the caller is still current.

        Args:
            client: the kazoo client checked out by the caller.

        Returns:
            True if it is the current underlying client holden by the
            manager; otherwise, False.

        """
        return client is self._client

    def get_client(self, no_throw=True):
        """Get the current underlying client, could be None.

        Args:
            no_throw: if the underlying client is None, throw exception or
                not.

        Returns:
            The underlying client kept by this manager.

        Throws:
            If no_throw is False, raise NoClientException when the
            underlying client is None.

        """
        if self._client is None and not no_throw:
            raise NoClientException()

        return self._client

    def on_client_change(self, func):
        """"Register a callback when the client has to be re-established.

        The underlying client is only stopped and recreated when it is messed
        up so badly that it cannot be repaired. In this case, we will have to
        tear down the existing client and try to recreate a new connection.
        Note that this should only happen very very rarely.

        Args:
            func: the callback function invoked when client is torn down and
                recreated.

        """
        self._client_callbacks.append(func)

    def _start(self, err_msg, spawn_monit=False):
        if self._is_destroyed:
            return

        self._client = None
        # Increase the session timeout from 10 to 25 seconds.
        try:
            host_list = self.zk_hosts
            client = KazooClient(
                hosts=",".join(host_list),
                timeout=self._get_session_timeout(),
                max_retries=3,
                handler=SequentialGeventHandler())

            # Increase the start timeout to 20 seconds from 15 seconds.
            # Guard this with explicit gevent timeout to protect us from
            # some corner cases where starting client failed to respect
            # start timeout passed in below.
            with gevent.Timeout(seconds=self._get_start_timeout() + 5):
                client.start(timeout=self._get_start_timeout())
            client.ensure_path("/")
            self._last_success_health_check_ts = time.time()
            log.info("Successfully started kazoo client.")
            self._client = client
        except (Exception, gevent.Timeout):
            self._sc.increment("errors.zk.client.start.failure",
                               tags={'host': hostname},
                               sample_rate=1)
            log.exception(err_msg)
        finally:
            if spawn_monit:
                self._monit_greenlet = gevent.spawn(self._monit)
                gevent.sleep(0)

    def _get_session_timeout(self):
        """Get zookeeper timeout setting.
        Returns:
            A float number as the number of seconds.

        """
        return self.session_timeout

    def _get_start_timeout(self):
        """Get zookeeper startup timeout setting.
        Returns:
            A float number as the number of seconds.

        """
        return self.start_timeout

    def _stop_client(self):
        """Best effort to stop the client."""
        try:
            # Make sure not to mistake this scenario with failing to stop
            # client.
            if self._client is None:
                log.info("Kazoo client is None.")
                return

            _retry((Exception,), tries=3, delay=1, backoff=2,
                   sleep_func=gevent.sleep)(self._client.stop)()

            log.info("Successfully stopped kazoo client.")
        except (Exception, gevent.Timeout):
            self._sc.increment("errors.zk.client.stop.failure",
                               tags={'host': hostname},
                               sample_rate=1)
            log.exception("Failed to stop kazoo client.")

    def _dispatch_client_change_callback(self, client):
        if self._is_destroyed:
            return
        log.info("Start dispatching client change callback.")
        for callback in self._client_callbacks:
            try:
                callback(client)
            except (Exception, gevent.Timeout):
                self._sc.increment("errors.zk.client.change_callback.failure",
                                   tags={'host': hostname},
                                   sample_rate=1)
                log.exception("Failed to exec client change callback.")

    def _reconnect(self):
        log.info("Try to reconnect to zk.")
        self._stop_client()
        if self._is_destroyed:
            return False
        self._start("Failed to reconnect to zk.")
        if self._client and self._client.connected:
            gevent.spawn(self._dispatch_client_change_callback, self._client)
            return True
        self._sc.increment("zk.client.reconnect.failure",
                           tags={'host': hostname},
                           sample_rate=1)
        return False

    def _log_zk_healthy(self):
        # Suppress zk health log.
        if (self._last_healthy_log_ts is None or
                (self._last_success_health_check_ts -
                 self._last_healthy_log_ts >
                 KazooClientManager.MIN_HEALTH_LOG_INTERVAL_IN_SECS)):
            log.info("Underlying zookeeper connection is healthy.")
            self._last_healthy_log_ts = self._last_success_health_check_ts

    def _monit(self):
        """Buddy greenlet to renew client when things are messed up badly.

        Every 5 seconds, this buddy greenlet wakes up to perform a health
        check on the underlying client, if this check fails 3 times in a
        row, it will attempt to drop the current client and establish a new
        client, and upon successfully establishing the new client, invoke the
        client change callbacks.

        Ideally we shouldn't have to do anything like this with managing
        connection to zk servers, however, during testing kazoo client
        sometimes gets into a state where it can never recover on its own.
        This should happen very very rarely.

        """
        num_failures = 0
        while True and not self._is_destroyed:
            try:
                reconnect = False

                # Check the healthiness of the client
                try:
                    if self._client:
                        self._client.exists("/")
                        # This only happens every 5 seconds.
                        self._last_success_health_check_ts = time.time()
                        self._log_zk_healthy()
                        # Reset the number of failures after a success
                        num_failures = 0
                    else:
                        reconnect = True
                except (Exception, gevent.Timeout):
                    self._sc.increment("errors.zk.health_check.failure",
                                       tags={'host': hostname},
                                       sample_rate=1)
                    log.exception("Failed to check existence of zk root: %d.",
                                  num_failures)
                    # Reset _last_healthy_log_ts in case of failure.
                    self._last_healthy_log_ts = None
                    # Client is not healthy
                    num_failures += 1
                    if num_failures >= self._max_num_consecutive_failures:
                        reconnect = True
                finally:
                    # If the client manager has lost connection to zookeeper
                    # for more than 5 mins turn on the gauge.
                    if num_failures:
                        unhealthy_duration_in_secs = (
                            time.time() - self._last_success_health_check_ts)
                        if unhealthy_duration_in_secs > 5 * 60:
                            # TODO: it'd be great if we can have
                            # gauge and hookup an alert with it, or it happens
                            # rarely enough, just kill this process and let
                            # supervisor restart it, but it feels a bit scary
                            # to do that, because that has the potential to
                            # bring down the whole service.
                            log.error(
                                "Failed to connect to zookeeper for %.2f "
                                "seconds", unhealthy_duration_in_secs)
                            # In this case, reconnect right now instead of
                            # waiting for three failures in a row. I've seen
                            # python process was suspended on ngapp for
                            # about 20 mins.
                            reconnect = True

                if reconnect:
                    if self._reconnect():
                        # Reset the number of failures experienced from monit
                        num_failures = 0

                gevent.sleep(self._health_check_interval)
            except gevent.GreenletExit:
                log.exception("Zookeeper connection monit is stopped.")
                raise
            except BaseException:
                # Just log the exception and keep going in this case.
                # We catch BaseException here because we dont want to have
                # this greenlet stop in any case other than this greenlet is
                # explicitly killed. In case of making the process respond to
                # SystemExit or KeyboardInterrupt, this greenlet should
                log.exception("Unexpected exception in zookeeper monit")
                # Give other greenlet a chance.
                gevent.sleep(0)

    def _destroy(self):
        """This will cause new attempts to reconnect to stop.

        However, this doesn't guarantee the current ongoing attempt to
        reconnect is stopped. And no futher underlying health check on the
        current client will be done.

        This should only be called when an endpoint is ready to be
        terminated.

        DONT CALL THIS METHOD WITHOUT CONSULTING SOMEONE IN INFROPS.

        """
        self._is_destroyed = True


class WatcherUtil(object):

    CLIENT_CHANGED = object()

    @staticmethod
    def _get_waiting_in_secs(waiting_in_secs,
                             num_retries,
                             max_waiting_in_secs):
        """Retrieve the waiting time in seconds.

        This method uses exponential back-off in figuring out the number of
        seconds to wait; however, the max wait time shouldn't be more than
        what is specified via max_waiting_in_seconds.

        Args:
            waiting_in_secs: waiting time in seconds.
            num_retries: number of retries, starting from 0.
            max_waiting_in_secs: maximum waiting time in seconds.

        Returns:
            The number of seconds to wait.

        """
        # make the backoff going up even faster
        waiting_in_secs *= 2**num_retries
        jitter = waiting_in_secs * 0.2
        waiting_in_secs += random.triangular(-jitter, jitter)
        return min(waiting_in_secs, max_waiting_in_secs)

    @staticmethod
    def _keep_retrying_till_client_change(
            zk_hosts, client, msg, waiting_in_secs, max_waiting_in_secs,
            is_stopped, func, *args, **kwargs):
        """Keep retrying the function until the underlying client is changed.

        If the function throws, this method will keep retrying the function
        until it succeeds or the underlying client maintained by
        KazooClientManager is changed.

        Args:
            zk_hosts: the zk endpoint list
            client: a kazoo client
            msg: the message to be logged when the function throws
            waiting_in_secs: the waiting time in seconds, when the operations
                on zookeeper fail.
            max_waiting_in_secs: the max number of seconds to wait.
            is_stoppped: Whether to stop trying or not.
            func: the function to be invoked
            args: the arguments to be passed into the function
            kwargs: the named arguments to be passed into the function

        Returns:
            The result of the function passed in, or None in case that the
            underlying client is changed.

        """
        num_retries = 0
        while ((not is_stopped or not is_stopped()) and
               KazooClientManager(zk_hosts).is_current_client(client)):
            try:
                if client is None:
                    log.info("Client is None, go to sleep.")
                    # When waiting for a client don't do exponential back-off
                    gevent.sleep(
                        WatcherUtil._get_waiting_in_secs(
                            waiting_in_secs, 0, max_waiting_in_secs))
                else:
                    num_retries += 1
                    result = func(client, *args, **kwargs)
                    log.info("%s succeeded.", func)
                    return result
            except (Exception, gevent.Timeout):
                log.exception(msg)
                gevent.sleep(WatcherUtil._get_waiting_in_secs(
                    waiting_in_secs, num_retries, max_waiting_in_secs))

        log.info("Kazoo client is changed.")
        return WatcherUtil.CLIENT_CHANGED

    @staticmethod
    def _do_it(zk_hosts, keep_retrying, keep_retrying_on_reconnect, err_msg,
               waiting_in_seconds, max_waiting_in_secs, is_stopped, command):
        """Handles the complexity of client reconnection and replacement.

        Args:
            zk_hosts: the zk endpoint list
            keep_retrying: whether to only perform the initial operation once
                or keep retrying until success; if keep_retrying is set to
                True, this ought to be done in a separate greenlet or thread
                to avoid possible indefinite blocking.
            keep_retrying_on_reconnect: some of the commands already handles
                client reconnection, in that case, this method doesn't need
                to handle the reconnection event any more.
            err_msg: the error message to be logged when the operation fails.
            waiting_in_secs: the waiting time in seconds, when the operations
                on zookeeper fail.
            max_waiting_in_secs: the max waiting time in seconds,
                when the operations on zookeeper fail.
            is_stopped: Whether to stop trying or not.
            command: the operation to be performed, can only be join,
                or monitor in the current implementation.

        """

        def on_reconnect(state):
            # Here we handle the client reconnection.
            if state == KazooState.CONNECTED:
                # Spawn to not block the session event handling greenlet
                gevent.spawn(WatcherUtil._keep_retrying_till_client_change,
                             zk_hosts,
                             KazooClientManager(zk_hosts).get_client(),
                             err_msg, waiting_in_seconds, max_waiting_in_secs,
                             is_stopped, command)

        def inner_do_it(keep_retrying, client):
            if not keep_retrying and (not is_stopped or not is_stopped()):
                # The initial invocation.
                result = command(client)
            else:
                # This path only invoked when client is replaced with a new
                # one, we need to keep retrying until it succeeds or the
                # underlying client is being replaced again.
                result = WatcherUtil._keep_retrying_till_client_change(
                    zk_hosts, client, err_msg, waiting_in_seconds,
                    max_waiting_in_secs, is_stopped, command)

            # register reconnection handler if needed.
            if (keep_retrying_on_reconnect and
                    result is not WatcherUtil.CLIENT_CHANGED and
                    client is not None):
                client.add_listener(on_reconnect)

            return result

        result = None

        # Pass in the partial function, when invoked, the current client at
        # the moment will be passed in, so the operation will be tried on
        # the new client instead.
        # We need to register this before doing the operation in the current
        # greenlet to avoid missing the client change event.
        KazooClientManager(zk_hosts).on_client_change(
            functools.partial(inner_do_it, True))

        while True:
            log.debug("Start processing %s.", command)
            client = KazooClientManager(zk_hosts).get_client()
            result = inner_do_it(keep_retrying, client)
            # Underlying client changed causes inner_do_it to return or we
            # choose not to keep retrying
            if client is KazooClientManager(zk_hosts).get_client():
                log.debug("Stop because the command succeeded: %s.", command)
                break

            if not keep_retrying:
                log.debug("Stop because no keeping retrying: %s.", command)
                break

        return result

    @staticmethod
    def spawn(keep_retrying, keep_retrying_on_reconnect, err_msg,
              waiting_in_seconds, command, zk_hosts, max_waiting_in_secs=300,
              is_stopped=None):
        """Spawn a greenlet to complete the operation on the server set.

        If keep_retrying is set to False, this method will block on the
        initial operation and raise whatever exception rasied in the initial
        operation.

        Args:
            keep_retrying: whether to keep retrying the initial operation
                until it succeeds.
            keep_retrying_on_reconnect: whether to keep retrying the
                operation upon client reconnection.
            err_msg: the error message to log when operation fails.
            waiting_in_secs: the waiting time in seconds, when the operations
                on zookeeper fail.
            max_waiting_in_secs: the max waiting time in seconds,
                when the operations on zookeeper fail.
            is_stopped: whether to keep trying or not.
            command: the operation to be done using the underlying client.
            zk_hosts: the zk hosts list.

        Returns:
            A greenlet, if keep_retrying is True, please avoid blocking on
            the returned greenlet indefinately.

        """
        g = gevent.spawn(
            WatcherUtil._do_it, zk_hosts, keep_retrying,
            keep_retrying_on_reconnect, err_msg, waiting_in_seconds,
            max_waiting_in_secs, is_stopped, command)
        if not keep_retrying:
            # wait till the greenlet is done and raise exception if needed
            g.get()
        return g


class DataWatcher(object):
    def __init__(self, path, zk_hosts, waiting_in_secs=5, sc=dummy_statsd,
                 file_path=None):
        """Constructor for the data watcher.

        Args:
            path: the path to the znode to watch the data change on.
            zk_hosts: the zookeeper endpoint list.
                the list should have the following format:
                ['observerzookeeper010:2181',
                 'observerzookeeper011:2181',
                 'observerzookeeper012:2181',
                 'observerzookeeper013:2181',
                 'observerzookeeper014:2181',
                 'observerzookeeper015:2181']
            waiting_in_secs: the waiting time in seconds,
                when the operations on zookeeper fail; by default, 5 second.
            sc: the statsd client
            file_path: the local file to watch. When it is set, the data
                watcher does not talk to zk for monitoring data changes.
        """
        self._path = path
        self._path_stats_name = _escape_path_for_stats_name(path)
        self._waiting_in_secs = waiting_in_secs
        self.zk_hosts = zk_hosts
        self._sc = sc
        self._file_path = file_path
        self._file_path_stats_name = _escape_path_for_stats_name(file_path)
        # Stat is for allowing to return the stat of data with attribute
        # 'version'
        self.Stat = namedtuple('Stat', ['version'])

    def get_data(self):
        """Get the data associated with the znode path. When a local file
           is set, the function returns data in the local file.
           Otherwise it returns the latest data in zookeeper.

        Returns:
            The data in the format of a tuple (value,
            `kazoo.protocol.states.ZnodeStat`) of the znode. When a local
            file is used, the stat contains a version attribute which
            is the same as the mtime of the file.

        Raises:
            `kazoo.exceptions.NoNodeError` if the node doesn't exist;
            `kazoo.exceptions.ZookeeperError` if the server returns a
            non-zero error code; `NoClientException` if the underlying
            client is not set up correctly yet by `KazooClientManager`.

            If the local file is set but does not exist or accessible,
            OSError is raised.

        """
        if self._file_path:
            with open(self._file_path, 'r') as f:
                value = f.read()
                # Set version to be the mtime of the file
                stat = self.Stat(version=os.path.getmtime(self._file_path))
            return (value, stat)
        else:
            client = KazooClientManager(self.zk_hosts).get_client(no_throw=False)
            return client.get(self._path)

    def watch(self, func, keep_retrying=True):
        """Register a callback for data changes.

        This method will spawn a greenlet to monitor the data in zk or local
        file. If the caller wants to try monitoring and apply custom error
        handling, the caller can set keep_retrying is set to False; otherwise,
        the caller should set keep_retrying to True, all the errors are
        transparent to the caller, the caller can just make the call and
        forget about it. The former gives the caller a chance to use a
        default list of servers if the initial attempt to monitor fails.

        If _file_path is provided when the DataWather is initialized, the
        callback is triggered on the file change, otherwise it is triggered
        on data change in the zookeeper path.

        Args:
            keep_retrying: whether to keep retrying to monitor the data
                initially, or just try once, if it throws exception,
                apply the custom exception handling
            func: On server set changes, func will be invoked with the value
            and stat of data, stat is supposed to have a 'version' field.

        Returns:
            A greenlet, if keep_retrying is True, please avoid blocking on
            the returned greenlet indefinitely.

        """
        if self._file_path:
            # NOTE: we use gevent.spawn instead of WatcherUtil.spawn for
            # file based datawatch to avoid accidentally instantiate zk
            # connection. DONT combine these two code path without serious
            # discussion with infra and ops.
            g = gevent.spawn(FileWatch(sc=self._sc).add_watch,
                             self._file_path,
                             func,
                             keep_retrying=keep_retrying,
                             backoff_in_secs=self._waiting_in_secs)
            if not keep_retrying:
                g.get()
            return g

        def kazoo_datawatch(client):
            try:
                log.info("Try to place zk data watch: %s.",
                         str(self._path))
                client.DataWatch(self._path, func, allow_session_lost=True)
                log.info("Successfully placed zk data watch: %s." % self._path)
            except (Exception, gevent.Timeout):
                self._sc.increment("errors.zk.datawatch.failure",
                                   tags={'path': self._path_stats_name},
                                   sample_rate=1)
                raise
        err_msg = "Failed to place zk data watch: %s." % self._path
        return WatcherUtil.spawn(keep_retrying, False, err_msg,
                                 self._waiting_in_secs, kazoo_datawatch,
                                 self.zk_hosts)


class ServerSet(object):
    def __init__(self, path, zk_hosts, waiting_in_secs=5, sc=dummy_statsd,
                 file_path=None):
        """Constructor for the server set.

        Args:
            zk_hosts: the zk hosts list.
            path: the root path of the server set.
            waiting_in_secs: the waiting time in seconds,
                when the operations on zookeeper fail; by default, 5 second.
            sc: the statsd client
            file_path: the local file to watch. When it is set, the server
                set does not talk to zk for monitoring data changes.
        """
        self._path = path
        self._path_stats_name = _escape_path_for_stats_name(path)
        self._waiting_in_secs = waiting_in_secs
        self._is_destroyed = False
        self.zk_hosts = zk_hosts
        self._sc = sc
        self._file_path = file_path
        self._file_path_stats_name = _escape_path_for_stats_name(file_path)

    def get_endpoints(self):
        """Retrieve the endpoints in the server set.

        Returns:
            The current endpoints in the server set in a list of strings of
            the format: host:port.

        Throws:
            NoClientException, when the underlying client is None;
            or Exception, since this method doesn't make effort to be
            bullet-proof, it will pass the exception thrown by the
            underlying client to the caller. Normally only join and monitor
            are interesting to the upstream callers.

        """
        client = KazooClientManager(self.zk_hosts).get_client(no_throw=False)
        party = client.ShallowParty(self._path)
        return set(party)

    @staticmethod
    def _create_endpoint(port, use_ip):
        host_name = socket.gethostname()
        ip_addr = socket.gethostbyname(host_name)
        if use_ip:
            return "%s:%d" % (ip_addr, port)
        else:
            return "%s:%d" % (host_name, port)

    def join(self, port, use_ip=True, keep_retrying=True, data=None, node_name=None):
        """If use_ip is set and if the hosts ip_addr starts with 10., this method
        joins the zk serverset using 'ip_addr:port' else it uses 'hostname:port'.

        This method will spawn a greenlet to join the server set. If the
        caller wants to try to join once and apply custom error handling, the
        caller can set keep_retrying is set to False; otherwise,
        the caller should set keep_retrying to True, all the errors are
        transparent to the caller, the caller can just make the call and
        forget about it.

        This method always talks to zookeeper directly, no matter the local
        file is provided or not.

        Args:
            port: The port when registering in zk as "host:port"
            use_ip: Use internal ip when registering in zk i.e. "ip:port"
            keep_retrying: whether to keep retrying to join the server
                set initially, or just try once.
            data: Data to save in the zk node. By default its the string "host:port" ("ip:port" if use_ip)
            node_name: Name of the zk node. By default its the string "host:port" ("ip:port" if use_ip)

        Returns:
            A greenlet, if keep_retrying is True, please avoid blocking on
            the returned greenlet indefinately. If keep_retrying is False,

        """
        endpoint = node_name if node_name else ServerSet._create_endpoint(port, use_ip)

        def is_destroyed():
            return self._is_destroyed

        def kazoo_join(client):
            if self._is_destroyed:
                return
            try:
                log.info(
                    "Try to join server set: %s, %s.", self._path, endpoint)
                party = client.ShallowParty(self._path, endpoint)
                if data:
                    party.data = data
                party.join()
                if self._is_destroyed:
                    children = client.get_children(self._path)
                    if children:
                        for child in children:
                            if child.endswith(endpoint):
                                client.delete(self._path + "/" + child)
                log.info("Successfully join server set: %s, %s.", self._path,
                         endpoint)
            except (Exception, gevent.Timeout):
                self._sc.increment("errors.zk.serverset.join.failure",
                                   tags={'path': self._path_stats_name},
                                   sample_rate=1)
                raise

        log.info("Spawn a greenlet to join serverset: %s, %s." % (self._path, endpoint))
        err_msg = "Failed to join server set: %s, %s." % (self._path, endpoint)
        return WatcherUtil.spawn(keep_retrying, True, err_msg,
                                 self._waiting_in_secs, kazoo_join,
                                 self.zk_hosts, is_stopped=is_destroyed)

    def monitor(self, func, keep_retrying=True):
        """Register a callback for server set changes.

        This method will spawn a greenlet to monitor the server set. If the
        caller wants to try monitoring and apply custom error handling, the
        caller can set keep_retrying is set to False; otherwise,
        the caller should set keep_retrying to True, all the errors are
        transparent to the caller, the caller can just make the call and
        forget about it. The former gives the caller a chance to use a
        default list of servers if the initial attempt to monitor fails.

        If _file_path is set, the serverset watches changes in the local file,
        otherwise it watches changes in zookeeper.

        Args:
            keep_retrying: whether to keep retrying to monitor the server
                set initially, or just try once, if it throws exception,
                apply the custom exception handling
            func: On server set changes, func will be invoked with a list of
                strings of the format: host:port.

        Returns:
            A greenlet, if keep_retrying is True, please avoid blocking on
            the returned greenlet indefinately.

        """
        server_set_type = 'zk'
        path_stats_name = self._path_stats_name
        path = self._path
        if self._file_path:
            server_set_type = 'file'
            path_stats_name = self._file_path_stats_name
            path = self._file_path

        def on_server_set_change(children):
            """The callback invoked when the server set changes.

            Args:
                children: When local file is not set, it is a list of strings
                in the format of XXX-host_name:port.
                When local file is set, hosts in the file are in the format
                of host_name:port
            """
            self._sc.increment(
                "{}.serverset.monitor.change".format(server_set_type),
                tags={'path': path_stats_name},
                sample_rate=0.0001)
            log.debug("Children change observed: %s, %s.", path, children)
            if not self._file_path:
                func((child[(child.find("-") + 1):] for child in children))
            else:
                func(children)

        if self._file_path:
            # NOTE: we use gevent.spawn instead of WatcherUtil.spawn for
            # file based serverset to avoid accidentally instantiate zk
            # connection. DONT combine these two code path without serious
            # discussion with infra and ops.
            g = gevent.spawn(FileWatch(sc=self._sc).add_watch,
                             self._file_path,
                             on_server_set_change,
                             watch_type='serverset',
                             keep_retrying=keep_retrying,
                             backoff_in_secs=self._waiting_in_secs)
            if not keep_retrying:
                g.get()
            return g

        def kazoo_monitor(client):
            try:
                log.debug("Try to place serverset zk watch: %s." % self._path)
                client.ChildrenWatch(
                    self._path, on_server_set_change, allow_session_lost=True)
                log.debug("Successfully placed children watch: %s.",
                          self._path)
            except (Exception, gevent.Timeout):
                self._sc.increment("errors.zk.serverset.monitor.failure",
                                   tags={'path': self._path_stats_name},
                                   sample_rate=1)
                raise

        err_msg = "Failed to monitor zk server set: %s." % self._path

        return WatcherUtil.spawn(keep_retrying, False, err_msg,
                                 self._waiting_in_secs, kazoo_monitor,
                                 self.zk_hosts)

    def _destroy(self, endpoint):
        """Best effort to take the endpoint out of serverset.

        However, there is no guaranttee that after this call the endpoint is
        out of the serverset right away. It is only guarantteed to be out of
        the serverset after session timeout.

        This should only be called when an endpoint is ready to be
        terminated.

        DONT CALL THIS METHOD WITHOUT CONSULTING SOMEONE IN INFROPS.

        """
        KazooClientManager(self.zk_hosts)._destroy()
        self._is_destroyed = True
        client = KazooClientManager(self.zk_hosts).get_client()
        if client:
            children = client.get_children(self._path)
            if children:
                for child in children:
                    if child.endswith(endpoint):
                        client.delete(self._path + "/" + child)


def _retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=log,
           sleep_func=time.sleep, max_delay=sys.maxint):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    Args:
        ExceptionToCheck: exception to check. May be a tuple of
            exceptions to check.
        tries: an integer, number of times to try (not retry) before
            giving up.
        delay: an integer, initial delay between retries in seconds.
        backoff: an integer, backoff multiplier e.g. value of 2 will
            double the delay each retry
        logger: logging.Logger instance, logger to use. By default,
            we use ``logging.log.log``; if None is explicitly specified by
            the caller, ``print`` is used.
        sleep_func: the sleep function to be used for waiting between
            retries. By default, it is ``time.sleep``,
            but it could also be gevent.sleep if we are using this with
            gevent.
        max_delay: the max number of seconds to wait between retries.

    Returns:
        Decorator function.

    """
    def deco_retry(f):

        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    log.warning(
                        "%s, Retrying in %d seconds...", e, mdelay)
                    sleep_func(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                    # Don't wait more than max_delay allowed
                    if mdelay > max_delay:
                        mdelay = max_delay
            return f(*args, **kwargs)

        return f_retry  # True decorator.

    return deco_retry
