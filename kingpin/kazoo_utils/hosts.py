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

"""Utility classes for choosing host to communicate with.

This module contains classes that can be used to easy communication with a
list of service hosts. For each host in a list an expiration and retry time
can be set. Hosts can be marked as "invalid" to be tried out connecting to
again after a timeout period.

"""
import logging
import os
from . import ServerSet
from .decorators import SingletonMetaclass
from .utils import dummy_statsd, hostname


import random
import time

from gevent.coros import RLock

log = logging.getLogger(__name__)


USE_ZOOKEEPER_FOR_DISCOVERY = os.environ.get(
    'USE_ZOOKEEPER_FOR_DISCOVERY', True)


class HostsProvider(object):
    """Provide a list of current live hosts."""
    def __init__(self, static_host_list, server_set_path="",
                 statsd_client=dummy_statsd, file_path=None):
        """Constructor.

        Args:
            static_host_list: The list of hosts providing this service as
                specified in the settings.
            server_set_path: The server set path to retrieve the current
                list of live hosts. If both this parameter and file_path are
                empty string or not set, it implies that we are not using
                server set. Cannot be used together with file_path.
            statsd_client: statsd client, if none is provided, a dummy
                client will be used.
            file_path: the local file path that contains the server set.
                Cannot be used together with server_set_path.

        """
        # Used when there is a problem with server set or this host provider
        # is not configured to use server set.
        if server_set_path and file_path:
            raise Exception("server_set_path and file_path cannot be both "
                            "provided.")

        self._static_host_tuple = tuple(static_host_list)
        # The current list of live hosts.
        self._current_host_tuple = tuple(static_host_list)
        # The zookeeper path to the server set.
        self._server_set_path = server_set_path
        # Whether this host provide has been appropriately initialized.
        self.initialized = False
        self.statsd = statsd_client
        self._file_path = file_path
        self._initialize()

    def __on_server_set_change(self, children):
        # Safe-guard us against duplicates.
        children_tuple = tuple(set(children))
        if not children_tuple:
            log.warn("The server set is empty for zookeeper path %s and "
                     "file path %s.",
                     self._server_set_path,
                     str(self._file_path))
            self.statsd.increment("errors.zk.empty_host_provider.{}".format(
                hostname, 1.0))
            # If the children set is empty, leave the current host list alone.
            return
        self._current_host_tuple = children_tuple

    def _initialize(self):
        if self.initialized:
            return

        if not self._server_set_path and not self._file_path:
            self.initialized = True
            return

        if not USE_ZOOKEEPER_FOR_DISCOVERY:
            raise Exception("Error: USE_ZOOKEEPER_FOR_DISCOVERY is set to false")

        server_set = ServerSet(
            self._server_set_path, None, sc=self.statsd, file_path=self._file_path)
        # noinspection PyBroadException
        try:
            log.debug(
                "Try to place monitor on server set: %s and file path %s.",
                self._server_set_path,
                str(self._file_path))
            greenlet = server_set.monitor(self.__on_server_set_change,
                                          keep_retrying=False)
            # Wait till synchronous monitoring is either succeeded or
            # failed. If succeeded, _current_host_list is initialized
            # with the live hosts in the server set; if failed,
            # raise the exception in the monitoring greenlet.
            greenlet.get()
            log.debug("Successfully placed monitor on server set: %s and "
                      "file path %s.",
                      self._server_set_path,
                      str(self._file_path))
        except Exception:
            log.exception(
                "Ran into troubles in monitoring server set: %s.",
                self._server_set_path)
            log.info("Now keep trying to monitor server set  %s and file "
                     "path %s in the background.",
                     self._server_set_path,
                     str(self._file_path))
            # In case of problems in synchronous server set monitoring,
            # just keep retrying server set monitoring in the background.
            server_set.monitor(self.__on_server_set_change)
        finally:
            self.initialized = True

    @property
    def hosts(self):
        if not self.initialized:
            raise RuntimeError("Host provider not initialized yet.")

        if not self._current_host_tuple:
            # As a safety net, when there is no live host available,
            # use the static host list instead.
            return self._static_host_tuple

        return self._current_host_tuple

    def __str__(self):
        return "initialized: %s, path: %s, same as static: %s, hosts: %s" % (
            self.initialized,
            self._server_set_path,
            self._current_host_tuple == self._static_host_tuple,
            self._current_host_tuple)


class HostProviderDict(object):
    """A dictionary for all host providers.

    The intended usage is to have the caller retrieve the host provider for a
    given service by calling ``get``, if one is certain that such a host
    provider has already been registered. If not, ``get_and_register``
    should be called instead.

    """

    __metaclass__ = SingletonMetaclass

    def __init__(self):
        self._host_providers = {}
        self._lock = RLock()

    def get(self, name):
        """Get the host provider for the given name, could return None."""
        return self._host_providers.get(name)

    def get_and_register(self, name, static_host_list, server_set_path,
                         file_path, statsd_client=dummy_statsd):
        """Get the host provider for the given name; if None, register."""
        host_provider = self._host_providers.get(name)
        if host_provider is not None:
            return host_provider
        self.register(name, static_host_list, server_set_path,
                      statsd_client=statsd_client, file_path=file_path)
        return self._host_providers.get(name)

    def register(self, name, static_host_list, server_set_path=None,
                 statsd_client=dummy_statsd, file_path=None):
        """Register the host provider with this Dictionary.

        Args:
            name: The name of the host provider.
            static_host_list: The static list of hosts providing the service.
            server_set_path: The path to the server set on zookeeper. Only
            pass this in if you intend to use zookeeper based server set
            to get the live hosts.
            statsd_client: statsd client, if none is provided, a dummy
            client will be used.
            file_path: The local file path containing server set.

        """
        with self._lock:
            if name in self._host_providers:
                return
            log.debug("Register host provider for %s with server set path: %s,"
                      "file path: %s, and static host list: %s.",
                      name, server_set_path, str(file_path), static_host_list)
            host_provider = HostsProvider(
                static_host_list, server_set_path, statsd_client=statsd_client,
                file_path=file_path)
            self._host_providers[name] = host_provider

    def __str__(self):
        return ("\n".join('%s="%s"' % (k, v) for (k, v) in
                          self._host_providers.iteritems()))


class BaseHostSelector(object):
    """Abstract class that encapsulates base host selection steps.

    Ensures that hosts expire, go through a proper retry waiting cycle
    or get invalidated as needed. Choosing the new host is left for the
    derived class to implement.

    """

    def __init__(self, host_provider, expire_time=600, retry_time=60,
                 invalidation_threshold=0.2):
        """Initialize the host selection.

        Args:
            host_provider: A ``HostProvider``, used to get the current list
                of live hosts.
            expire_time: An integer, expire time in seconds.
            retry_time: An integer, retry time in seconds.
            invalidation_threshold: A float, when the number of entries
                being invalidated divided by the number of all valid hosts
                is above this threshold, we stop accepting invalidation
                requests. We do this to stay on the conservative side to
                avoid invalidating hosts too fast to starve requests.

        """
        assert host_provider.initialized
        # Current host.
        self._current = None
        # Last host, works even when current host invalided.
        self._last = None
        # Time when we selected the current host.
        self._select_time = None
        # Adjust expire time by +/- 10%, but 0 is special for testing purpose.
        self._expire_time = expire_time
        if expire_time:
            self._expire_time = expire_time + int(
                random.triangular(-expire_time * 0.1, expire_time * 0.1))
        self._retry_time = retry_time
        self._invalidation_threshold = invalidation_threshold
        # Host name -> time when marked bad.
        self._bad_hosts = {}
        self._host_provider = host_provider

    def get_host(self):
        """Get a new host to use.

        New host is internally saved.

        Returns:
            A string.

        """
        self._ensure_host()
        log.debug("Select host: %s.", self._current)
        assert self._current
        return self._current

    def get_last_host(self):
        """Get last host used.

        Returns:
            A string.

        """
        return self._last

    def _ensure_host(self, current_time=None):
        """Get new host to use, and mark as bad expired hosts.

        Args:
            time: set by default to time module, used for testing only and
                not to be specified in normal use.

        """
        if current_time is None:
            current_time = time.time()

        # Go through a copy of the invalidated host list, remove any
        # host that we should try again (based on retry_time).
        for key, marked_time in self._bad_hosts.items():
            if current_time - marked_time > self._retry_time:
                log.info("Evict %s from bad hosts list.", key)
                del self._bad_hosts[key]

        # Pick a new host every X seconds, or if expire_time is set to 0,
        # always use a different host.
        choose_another = False
        if self._expire_time == 0 or self._select_time is None or (
                self._select_time and
                current_time - self._select_time > self._expire_time):
            choose_another = True
        else:
            assert self._current

        if choose_another:
            chosen = self._choose_host()

            self._last = self._current
            self._current = chosen
            self._select_time = current_time

    def invalidate(self, current_time=None):
        """Invalidate current host.

        Args:
            time: A float, time in seconds since epoch, used for testing only
                and not to be specified in normal use.

        """
        # Remember when we marked this host.
        if current_time is not None:
            # Make sure a float is passed in, or something that can be
            # converted to float.
            current_time = float(current_time)
            real_current_time = time.time()
            # We don't want to allow future time here; other than that,
            # everything else it is okay, the worst thing could happen is
            # that the bad host gets evicted before it is actually due.
            if current_time > real_current_time:
                current_time = real_current_time
        else:
            current_time = time.time()
        if self._current not in self._bad_hosts:
            thresold = (
                len(self._host_provider.hosts) * self._invalidation_threshold)
            # Here is where we use discretion on whether to accept the
            # invalidation request.
            if len(self._bad_hosts) + 1 <= thresold:
                self._bad_hosts[self._current] = current_time
                log.info("Invalidate host: %s at %f.", self._current,
                         current_time)
            else:
                log.warning("Reject host invalidation: %s", self._current)
        self._current = None

    def _choose_host(self):
        """Abstract method, determines how to chose the next host.

        Derived classes are to override the method.

        Returns:
             A live host to the best knowledge. The return result should be
             a string of the format: host:port.

        """
        raise NotImplementedError()


class RandomHostSelector(BaseHostSelector):
    """Host selector with random host selection."""

    def __init__(self, host_provider, expire_time=600, retry_time=60,
                 invalidation_threshold=0.2):
        """Random host selector with random host selection initialization.

        Args:
            host_provider: A ``HostProvider``, from which a set of currently
                live hosts can be obtained.
            expire_time: An integer, expire time in seconds.
            retry_time: An integer, retry time in seconds.
            invalidation_threshold: A float, when the number of entries
                being invalidated divided by the number of all valid hosts
                is above this threshold, we stop accepting invalidation
                requests. We do this to stay on the conversative side to
                avoid invalidating hosts too fast to starve requests.

        """
        super(RandomHostSelector, self).__init__(
            host_provider, expire_time=expire_time, retry_time=retry_time,
            invalidation_threshold=invalidation_threshold)

    def _choose_host(self):
        """Chose a random host from the list."""
        # TODO: there may be a better place to put the bad hosts
        # checking logic.
        # Retry up to three time to get a live host.
        if not self._host_provider.hosts:
            raise Exception('Serverset has no host. Please check if file based serverset is created.')

        for i in range(3):
            host = random.choice(self._host_provider.hosts)
            assert host
            if host not in self._bad_hosts:
                log.debug("Choose host: %s.", host)
                return host

        # Now the more expensive way.
        # Has to be a list instead of generator here to use random.choice.
        good_hosts = [host for host in self._host_provider.hosts
                      if host not in self._bad_hosts]
        host = random.choice(good_hosts)
        assert host
        log.info("Choose host via the expensive way: %s.", host)
        return host
