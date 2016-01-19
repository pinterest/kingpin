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

"""Tests for classes is common/hosts.py."""
from collections import Counter
from mock import Mock, patch
import os
import tempfile
import testutil
import time

from unittest import TestCase

from kingpin.kazoo_utils import KazooClientManager, ServerSet, hosts, FileWatch
from kingpin.kazoo_utils.hosts import (BaseHostSelector, HostsProvider, RandomHostSelector)

ZK_HOSTS = ["datazk001:2181", "datazk002:2181"]


class HostSelectorTestCase(TestCase):
    HOST_LIST = ["host1:8080", "host2:8181"]
    PORT_LIST = [8080, 8181]
    HOST_PROVIDER_NAME = "test"
    SERVER_SET_PATH = "/test_host_selector"

    def setUp(self):
        super(HostSelectorTestCase, self).setUp()
        hosts.USE_ZOOKEEPER_FOR_DISCOVERY = True

    def test_init_base_host_selector_class(self):
        """Test base initialization and functionality."""
        host_provider = HostsProvider([])
        base_host_selector = BaseHostSelector(host_provider)
        # Check that some base states are set.
        self.assertTrue(base_host_selector._last is None)
        self.assertTrue(base_host_selector._current is None)
        self.assertTrue(base_host_selector._select_time is None)
        self.assertEquals(base_host_selector._bad_hosts, {})
        self.assertEquals(base_host_selector._retry_time, 60)
        self.assertTrue(base_host_selector._host_provider is host_provider)

        # This is an abstract class. _chose_host() should raise an exception.
        self.assertRaises(NotImplementedError, base_host_selector._choose_host)

    def test_retrieving_and_invalidation(self):
        """Test host retrieval."""
        host_provider = HostsProvider(HostSelectorTestCase.HOST_LIST)
        base_host_selector = BaseHostSelector(
            host_provider, expire_time=0, retry_time=0,
            invalidation_threshold=1.0)
        self.assertTrue(base_host_selector.get_last_host() is None)

        with patch(hosts.__name__ + ".BaseHostSelector._choose_host",
                   new=Mock(return_value=HostSelectorTestCase.HOST_LIST[0])):
            # Get one host.
            host1 = base_host_selector.get_host()
            self.assertEquals(host1, HostSelectorTestCase.HOST_LIST[0])
            # If invalidated the state of the object changes.
            self.assertTrue(host1 not in base_host_selector._bad_hosts)
            base_host_selector.invalidate()
            self.assertTrue(host1 in base_host_selector._bad_hosts)

        # If called again, with retry_time being set to 0 bad hosts should be
        # invalidated.
        with patch(hosts.__name__ + ".BaseHostSelector._choose_host",
                   new=Mock(return_value=HostSelectorTestCase.HOST_LIST[1])):
            host2 = base_host_selector.get_host()
            # Now bad hosts should be empty
            self.assertTrue(not base_host_selector._bad_hosts)
            self.assertEquals(host2, HostSelectorTestCase.HOST_LIST[1])
            base_host_selector.invalidate()
            self.assertTrue(host2 in base_host_selector._bad_hosts)

    def test_reject_invalidation(self):
        """Test rejecting invalidation."""
        host_provider = HostsProvider(HostSelectorTestCase.HOST_LIST)
        base_host_selector = BaseHostSelector(host_provider, expire_time=0,
                                              retry_time=0)
        with patch(hosts.__name__ + ".BaseHostSelector._choose_host",
                   new=Mock(return_value=HostSelectorTestCase.HOST_LIST[0])):
            # Get one host.
            host1 = base_host_selector.get_host()
            self.assertEquals(host1, HostSelectorTestCase.HOST_LIST[0])
            # If invalidated the state of the object changes.
            self.assertTrue(host1 not in base_host_selector._bad_hosts)
            base_host_selector.invalidate()
            # Because 1 is larger than 2 * 0.2 = 0.4
            self.assertTrue(host1 not in base_host_selector._bad_hosts)
            base_host_selector._invalidation_threshold = 0.5
            host1 = base_host_selector.get_host()
            self.assertEquals(host1, HostSelectorTestCase.HOST_LIST[0])
            base_host_selector.invalidate()
            # Because 1 <= 2 * 0.5 = 1.0
            self.assertTrue(host1 in base_host_selector._bad_hosts)

    def test_random_host_selector(self):
        """Test the RandomHostSelector."""
        host_provider = HostsProvider(HostSelectorTestCase.HOST_LIST)
        random_host_selector = RandomHostSelector(
            host_provider, expire_time=0, retry_time=0,
            invalidation_threshold=1.0)

        # Note that we didn't have to mock _chose_host() call this time,
        # it should be im RandomHostSelector class already.
        some_host = random_host_selector.get_host()
        self.assertTrue(some_host in HostSelectorTestCase.HOST_LIST)
        self.assertEquals(random_host_selector._current, some_host)

        no_of_iterations = 250
        # If I run get_host() about 100 times I expect to have relatively
        # even distribution and all hosts in the host_list returned by now.
        returned_hosts = [random_host_selector.get_host()
                          for i in xrange(no_of_iterations)]
        host_counter = Counter(returned_hosts)

        # We expect that all calls happened.
        self.assertEquals(sum(host_counter.itervalues()), no_of_iterations)
        # We should have seen all the elements.
        self.assertEquals(set(host_counter),
                          set(HostSelectorTestCase.HOST_LIST))

        # But if we had left large expire_time only one host would be picked
        # up all the time, and we'll show that here.
        random_host_selector = RandomHostSelector(host_provider,
                                                  invalidation_threshold=1.0)
        returned_hosts = [random_host_selector.get_host()
                          for i in xrange(no_of_iterations)]
        host_counter = Counter(returned_hosts)
        self.assertEquals(len(list(host_counter)), 1)

        # Test invalidation
        hosts = [HostSelectorTestCase.HOST_LIST[0]]
        for i in xrange(4):
            hosts.append(HostSelectorTestCase.HOST_LIST[1])

        def random_select(*args):
            return hosts.pop()

        mock = Mock(side_effect=random_select)
        with patch("random.choice", new=mock):
            random_host_selector = RandomHostSelector(
                host_provider, expire_time=0, retry_time=60,
                invalidation_threshold=1.0)
            host = random_host_selector.get_host()
            self.assertEqual(host, HostSelectorTestCase.HOST_LIST[1])
            random_host_selector.invalidate()
            # Because mock will return the bad host three times in a row,
            # this will force it to compute the set of good hosts
            host = random_host_selector.get_host()
            self.assertEqual(host, HostSelectorTestCase.HOST_LIST[0])
            # At this point, random.choice should have been called 5 times
            self.assertEqual(mock.call_count, 5)

    @patch("kazoo.client.KazooClient.__new__",
           new=Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_random_host_selector_with_serverset(self):
        testutil.initialize_kazoo_client_manager(ZK_HOSTS)
        kazoo_client = KazooClientManager().get_client()
        kazoo_client.ensure_path(HostSelectorTestCase.SERVER_SET_PATH)
        host_provider = HostsProvider(HostSelectorTestCase.PORT_LIST,
                                      HostSelectorTestCase.SERVER_SET_PATH)
        self.assertTrue(host_provider.initialized)
        self.assertTrue(host_provider.hosts)
        # Since there is no live hosts in the server set, host provider should
        # still use the static host list.
        self.assertEqual(host_provider._current_host_tuple,
                         host_provider._static_host_tuple)
        random_host_selector = RandomHostSelector(
            host_provider, expire_time=0, retry_time=0,
            invalidation_threshold=1.0)
        self.assertTrue(random_host_selector.get_host() in
                        HostSelectorTestCase.PORT_LIST)
        server_set = ServerSet(HostSelectorTestCase.SERVER_SET_PATH, ZK_HOSTS)
        g = server_set.join(HostSelectorTestCase.PORT_LIST[0], use_ip=False)
        g.get()
        no_of_iterations = 100
        # After the first endpoint joins, random host selector should only
        # start to use hosts in the server set.
        returned_hosts = [random_host_selector.get_host()
                          for i in xrange(no_of_iterations)]
        self.assertEqual(len(set(returned_hosts)), 1)
        self.assertEqual(len(host_provider.hosts), 1)
        g = server_set.join(HostSelectorTestCase.PORT_LIST[1], use_ip=False)
        g.get()
        # After the second endpoint joins the server set, random host selector
        # should return both endpoints now.
        returned_hosts = [random_host_selector.get_host()
                          for i in xrange(no_of_iterations)]
        self.assertEqual(len(set(returned_hosts)), 2)
        self.assertEqual(len(host_provider.hosts), 2)

    def test_invalid_use_zk_for_discovery(self):
        """ Testing invalid USE_ZOOKEEPER_FOR_DISCOVERY """
        hosts.USE_ZOOKEEPER_FOR_DISCOVERY = False
        self.assertRaises(Exception, HostsProvider,
                          HostSelectorTestCase.HOST_LIST, "/")


class HostSelectorWithLocalFileTestCase(TestCase):
    """
    This class has exact test set as the class above. Every time a
    HostProvider is initialized, it takes an additional file path
    argument. Although adding this file path argument does not change
    the code path of all unit tests, we want to keep the exact test set
    here to make sure having the local file does not change any behavior
    of HostProvider.
    """
    HOST_LIST = ["host11:8080", "host12:8181"]
    HOST_PROVIDER_NAME = "test_provider"
    # Initialize a singleton file watch with low wait time
    FILE_WATCH = FileWatch(polling_wait_in_seconds=0.5)

    def setUp(self):
        super(HostSelectorWithLocalFileTestCase, self).setUp()
        hosts.USE_ZOOKEEPER_FOR_DISCOVERY = True

    def test_init_base_host_selector_class(self):
        """Test base initialization and functionality."""

        fd, tmp_file = tempfile.mkstemp()
        host_provider = HostsProvider([], file_path=tmp_file)
        base_host_selector = BaseHostSelector(host_provider)
        # Check that some base states are set.
        self.assertTrue(base_host_selector._last is None)
        self.assertTrue(base_host_selector._current is None)
        self.assertTrue(base_host_selector._select_time is None)
        self.assertEquals(base_host_selector._bad_hosts, {})
        self.assertEquals(base_host_selector._retry_time, 60)
        self.assertTrue(base_host_selector._host_provider is host_provider)

        # This is an abstract class. _chose_host() should raise an exception.
        self.assertRaises(NotImplementedError, base_host_selector._choose_host)
        HostSelectorWithLocalFileTestCase.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)

    def test_retrieving_and_invalidation(self):
        """Test host retrieval."""

        fd, tmp_file = tempfile.mkstemp()
        with open(tmp_file, 'w') as f:
            f.write('\n'.join(HostSelectorWithLocalFileTestCase.HOST_LIST))
        host_provider = HostsProvider(HostSelectorWithLocalFileTestCase.HOST_LIST, file_path=tmp_file)
        base_host_selector = BaseHostSelector(
            host_provider, expire_time=0, retry_time=0,
            invalidation_threshold=1.0)
        self.assertTrue(base_host_selector.get_last_host() is None)

        with patch(hosts.__name__ + ".BaseHostSelector._choose_host",
                   new=Mock(return_value=HostSelectorWithLocalFileTestCase.HOST_LIST[0])):
            # Get one host.
            host1 = base_host_selector.get_host()
            self.assertEquals(host1, HostSelectorWithLocalFileTestCase.HOST_LIST[0])
            # If invalidated the state of the object changes.
            self.assertTrue(host1 not in base_host_selector._bad_hosts)
            base_host_selector.invalidate()
            self.assertTrue(host1 in base_host_selector._bad_hosts)

        # If called again, with retry_time being set to 0 bad hosts should be
        # invalidated.
        with patch(hosts.__name__ + ".BaseHostSelector._choose_host",
                   new=Mock(return_value=HostSelectorWithLocalFileTestCase.HOST_LIST[1])):
            host2 = base_host_selector.get_host()
            # Now bad hosts should be empty
            self.assertTrue(not base_host_selector._bad_hosts)
            self.assertEquals(host2, HostSelectorWithLocalFileTestCase.HOST_LIST[1])
            base_host_selector.invalidate()
            self.assertTrue(host2 in base_host_selector._bad_hosts)
        HostSelectorWithLocalFileTestCase.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)

    def test_reject_invalidation(self):
        """Test rejecting invalidation."""
        fd, tmp_file = tempfile.mkstemp()
        with open(tmp_file, 'w') as f:
            f.write('\n'.join(HostSelectorWithLocalFileTestCase.HOST_LIST))
        host_provider = HostsProvider(HostSelectorWithLocalFileTestCase.HOST_LIST, file_path=tmp_file)
        base_host_selector = BaseHostSelector(host_provider, expire_time=0, retry_time=0)
        with patch(hosts.__name__ + ".BaseHostSelector._choose_host",
                   new=Mock(return_value=HostSelectorWithLocalFileTestCase.HOST_LIST[0])):
            # Get one host.
            host1 = base_host_selector.get_host()
            self.assertEquals(host1, HostSelectorWithLocalFileTestCase.HOST_LIST[0])
            # If invalidated the state of the object changes.
            self.assertTrue(host1 not in base_host_selector._bad_hosts)
            base_host_selector.invalidate()
            # Because 1 is larger than 2 * 0.2 = 0.4
            self.assertTrue(host1 not in base_host_selector._bad_hosts)
            base_host_selector._invalidation_threshold = 0.5
            host1 = base_host_selector.get_host()
            self.assertEquals(host1, HostSelectorWithLocalFileTestCase.HOST_LIST[0])
            base_host_selector.invalidate()
            # Because 1 <= 2 * 0.5 = 1.0
            self.assertTrue(host1 in base_host_selector._bad_hosts)
        HostSelectorWithLocalFileTestCase.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)

    def test_random_host_selector(self):
        """Test the RandomHostSelector."""
        fd, tmp_file = tempfile.mkstemp()
        with open(tmp_file, 'w') as f:
            f.write('\n'.join(HostSelectorWithLocalFileTestCase.HOST_LIST))

        host_provider = HostsProvider(HostSelectorWithLocalFileTestCase.HOST_LIST,
                                      file_path=tmp_file)
        random_host_selector = RandomHostSelector(
            host_provider, expire_time=0, retry_time=0,
            invalidation_threshold=1.0)

        # Note that we didn't have to mock _chose_host() call this time,
        # it should be im RandomHostSelector class already.
        some_host = random_host_selector.get_host()
        self.assertTrue(some_host in HostSelectorWithLocalFileTestCase.HOST_LIST)
        self.assertEquals(random_host_selector._current, some_host)

        no_of_iterations = 250
        # If I run get_host() about 100 times I expect to have relatively
        # even distribution and all hosts in the host_list returned by now.
        returned_hosts = [random_host_selector.get_host()
                          for i in xrange(no_of_iterations)]
        host_counter = Counter(returned_hosts)

        # We expect that all calls happened.
        self.assertEquals(sum(host_counter.itervalues()), no_of_iterations)
        # We should have seen all the elements.
        self.assertEquals(set(host_counter),
                          set(HostSelectorWithLocalFileTestCase.HOST_LIST))

        # But if we had left large expire_time only one host would be picked
        # up all the time, and we'll show that here.
        random_host_selector = RandomHostSelector(host_provider,
                                                  invalidation_threshold=1.0)
        returned_hosts = [random_host_selector.get_host()
                          for i in xrange(no_of_iterations)]
        host_counter = Counter(returned_hosts)
        self.assertEquals(len(list(host_counter)), 1)

        # Test invalidation
        hosts = [HostSelectorWithLocalFileTestCase.HOST_LIST[0]]
        for i in xrange(4):
            hosts.append(HostSelectorWithLocalFileTestCase.HOST_LIST[1])

        def random_select(*args):
            return hosts.pop()

        mock = Mock(side_effect=random_select)
        with patch("random.choice", new=mock):
            random_host_selector = RandomHostSelector(
                host_provider, expire_time=0, retry_time=60,
                invalidation_threshold=1.0)
            host = random_host_selector.get_host()
            self.assertEqual(host, HostSelectorWithLocalFileTestCase.HOST_LIST[1])
            random_host_selector.invalidate()
            # Because mock will return the bad host three times in a row,
            # this will force it to compute the set of good hosts
            host = random_host_selector.get_host()
            self.assertEqual(host, HostSelectorWithLocalFileTestCase.HOST_LIST[0])
            # At this point, random.choice should have been called 5 times
            self.assertEqual(mock.call_count, 5)
        HostSelectorWithLocalFileTestCase.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)

    def test_random_host_selector_with_serverset(self):
        fd, tmp_file = tempfile.mkstemp()
        # Add a new host into the local server set file to simulate a join
        f = open(tmp_file, 'w')
        f.write(HostSelectorWithLocalFileTestCase.HOST_LIST[0])
        f.close()
        HostSelectorWithLocalFileTestCase.FILE_WATCH._check_file_updates()
        host_provider = HostsProvider(
            HostSelectorWithLocalFileTestCase.HOST_LIST, file_path=tmp_file)
        self.assertTrue(host_provider.initialized)
        self.assertTrue(host_provider.hosts)
        self.assertEqual(host_provider._current_host_tuple,
                         (HostSelectorWithLocalFileTestCase.HOST_LIST[0],))
        random_host_selector = RandomHostSelector(
            host_provider, expire_time=0, retry_time=0,
            invalidation_threshold=1.0)
        self.assertTrue(random_host_selector.get_host() in
                        HostSelectorWithLocalFileTestCase.HOST_LIST)

        no_of_iterations = 100
        # After the first endpoint joins, random host selector should only
        # start to use hosts in the server set.
        returned_hosts = [random_host_selector.get_host()
                          for i in xrange(no_of_iterations)]
        self.assertEqual(len(set(returned_hosts)), 1)
        self.assertEqual(len(host_provider.hosts), 1)
        time.sleep(1)
        f = open(tmp_file, 'a')
        f.write('\n' + HostSelectorWithLocalFileTestCase.HOST_LIST[1])
        f.close()
        HostSelectorWithLocalFileTestCase.FILE_WATCH._check_file_updates()
        # After the second endpoint joins the server set, random host selector
        # should return both endpoints now.
        returned_hosts = [random_host_selector.get_host()
                          for i in xrange(no_of_iterations)]
        self.assertEqual(len(set(returned_hosts)), 2)
        self.assertEqual(len(host_provider.hosts), 2)
        HostSelectorWithLocalFileTestCase.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)

    def test_invalid_use_zk_for_discovery(self):
        """Test invalid USE_ZOOKEEPER_FOR_DISCOVERY setting."""
        fd, tmp_file = tempfile.mkstemp()
        hosts.USE_ZOOKEEPER_FOR_DISCOVERY = False
        self.assertRaises(Exception, HostsProvider,
                          HostSelectorWithLocalFileTestCase.HOST_LIST,
                          file_path = tmp_file)
        HostSelectorWithLocalFileTestCase.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)

    def test_both_zk_and_file_paths(self):
        """Test invalid USE_ZOOKEEPER_FOR_DISCOVERY setting."""
        fd, tmp_file = tempfile.mkstemp()
        hosts.USE_ZOOKEEPER_FOR_DISCOVERY = False
        self.assertRaises(Exception, HostsProvider,
                          HostSelectorWithLocalFileTestCase.HOST_LIST,
                          "/foo",
                          file_path = tmp_file)
        HostSelectorWithLocalFileTestCase.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)
