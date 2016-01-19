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

"""Test cases for ServerSet."""
import socket
import os
import tempfile

import gevent
from gevent.event import Event
import mock
from mock import Mock, patch
from nose.plugins.attrib import attr
import unittest

from kingpin.kazoo_utils import DataWatcher, KazooClientManager, ServerSet
from kingpin.kazoo_utils.file_watch import FileWatch
import testutil

ZK_HOSTS = ["datazk001:2181", "datazk002:2181"]


class KazooClientManagerSingletonTestCase(unittest.TestCase):
    @patch("kazoo.client.KazooClient.__new__",
           new=Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_get_zk_hosts_directly(self):
        """ Test passing zk_hosts in directly.
        """
        testutil.initialize_kazoo_client_manager(ZK_HOSTS)
        kz_client_manager = KazooClientManager(ZK_HOSTS)
        self.assertEqual(kz_client_manager.get_client().hosts, ",".join(ZK_HOSTS))


class DataWatcherTestCase(unittest.TestCase):
    TEST_PATH = "/test_data_watcher"
    DATA_0 = "foo"
    DATA_1 = "bar"


    @mock.patch("kazoo.client.KazooClient.__new__",
                new=mock.Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_data_watcher(self):
        """Test various scenarios for data watcher:

        1. When data get changed, watcher callback should be invoked.
        2. When the underlying zk client disconnects and then recovers,
           the watcher callback should be invoked.
        3. When the underlying zk client messes up beyond recovery,
           the underlying client should be replaced, and once the new client
           is in place, the watcher callback should be invoked again.

        """
        data_stat = []
        watcher_triggered = Event()

        def data_watch(data, stat):
            while data_stat:
                data_stat.pop()
            data_stat.append(data)
            data_stat.append(stat)
            watcher_triggered.set()

        testutil.initialize_kazoo_client_manager(ZK_HOSTS)
        client = KazooClientManager().get_client()
        client.create(DataWatcherTestCase.TEST_PATH,
                      DataWatcherTestCase.DATA_0)
        data_watcher = DataWatcher(DataWatcherTestCase.TEST_PATH,
                                   ZK_HOSTS,
                                   waiting_in_secs=0.01)
        data_watcher.watch(data_watch).join()
        watcher_triggered.wait(1)
        # Now the data and version should be foo and 0.
        self.assertEqual(data_stat[0], DataWatcherTestCase.DATA_0)
        self.assertEqual(data_stat[1].version, 0)
        watcher_triggered.clear()
        client.set(DataWatcherTestCase.TEST_PATH, DataWatcherTestCase.DATA_1)
        watcher_triggered.wait(1)
        # Make sure that watch callback is triggered.
        self.assertEqual(data_stat[0], DataWatcherTestCase.DATA_1)
        self.assertEqual(data_stat[1].version, 1)
        data_stat.pop()
        data_stat.pop()
        # Test recoverable failure
        watcher_triggered.clear()
        client.stop()
        client.start()
        # Here the client actually will call check the znode in the
        # background.
        watcher_triggered.wait(1)
        # Since nothing changed, no notification from the client.
        self.assertFalse(data_stat)
        # Test client change
        client.stop()
        watcher_triggered.clear()
        # give the monit greenlet a chance to detect failures.
        gevent.sleep(1)
        # Assert the client has been replaced with a new one.
        self.assertFalse(KazooClientManager().get_client() is client)
        watcher_triggered.wait(1)
        # Make sure that watch callback is triggered when client is replaced.
        self.assertEqual(data_stat[0], DataWatcherTestCase.DATA_1)
        self.assertEqual(data_stat[1].version, 1)


class DataWatcherWithFileTestCase(unittest.TestCase):
    """
    Test the data watcher with a local file provided.
    """
    TEST_PATH = "/test_data_watcher"
    DATA_0 = "foo"
    DATA_1 = "bar"

    # Initialize a singleton file watch with low wait time
    FILE_WATCH = FileWatch(polling_wait_in_seconds=0.5)

    @mock.patch("kazoo.client.KazooClient.__new__",
                new=mock.Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_data_watcher(self):
        """Test data watcher with a local file:

        1. When data get changed, watcher callback should be invoked.
        2. When the underlying zk client disconnects and then recovers,
           the watcher callback should be invoked.
        3. When the underlying zk client messes up beyond recovery,
           the underlying client should be replaced, and once the new client
           is in place, the watcher callback should be invoked again.

        Although when a local file is being watched, now all the code paths
        about the above behaviors got affected, we still want to test all the
        scenarios to make sure nothing breaks when a file is used.
        """
        data_stat = []
        watcher_triggered = Event()

        fd, tmp_file = tempfile.mkstemp()
        with open(tmp_file, 'w') as f:
            f.write(self.DATA_0)

        def data_watch(data, stat):
            while data_stat:
                data_stat.pop()
            data_stat.append(data)
            data_stat.append(stat)
            watcher_triggered.set()

        data_watcher = DataWatcher(DataWatcherWithFileTestCase.TEST_PATH,
                                   ZK_HOSTS,
                                   waiting_in_secs=0.01,
                                   file_path=tmp_file)
        data_watcher.watch(data_watch).join()
        watcher_triggered.wait(1)
        # Now the data and version should be foo and the mtime of file.
        mtime = os.path.getmtime(tmp_file)
        self.assertEqual(data_stat[0], DataWatcherWithFileTestCase.DATA_0)
        self.assertEqual(data_stat[1].version, mtime)
        self.assertEqual(data_watcher.get_data()[0], DataWatcherWithFileTestCase.DATA_0)
        self.assertEqual(data_watcher.get_data()[1].version, mtime)
        watcher_triggered.clear()

        gevent.sleep(1)
        with open(tmp_file, 'w') as f:
            f.write(self.DATA_1)
        watcher_triggered.wait(1)
        # Make sure that watch callback is triggered.
        mtime = os.path.getmtime(tmp_file)
        self.assertEqual(data_stat[0], DataWatcherWithFileTestCase.DATA_1)
        self.assertEqual(data_stat[1].version, mtime)
        self.assertEqual(data_watcher.get_data()[0], DataWatcherWithFileTestCase.DATA_1)
        self.assertEqual(data_watcher.get_data()[1].version, mtime)
        data_stat.pop()
        data_stat.pop()

        # Test recoverable failure, even though the watcher with a file path
        # is not changing any implementation or behavior in this part, we want
        # to keep the tests here to ensure.
        watcher_triggered.clear()

        self.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)


class ServerSetTestCase(unittest.TestCase):
    """Test server set."""

    SERVER_SET_PATH = "/test_server_set"
    SERVER_SET_DESTROY_PATH = "/test_server_set_destroy"
    PORT_1 = 8080
    PORT_2 = 8081
    END_POINT_1 = ServerSet._create_endpoint(PORT_1, True)
    END_POINT_2 = ServerSet._create_endpoint(PORT_2, True)
    END_POINTS = [END_POINT_1, END_POINT_2]

    @mock.patch("kazoo.client.KazooClient.__new__",
                new=mock.Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_server_set(self):
        """Test various failure scenarios on server set implementation.

        1. When a new server joins the set, the watcher should be notified.
        2. When the underlying zk client disconnects and then recovers,
           the server set should be transparent to server set participants
           and watchers.
        3. When the underlying zk client messes up beyond recovery,
           the underlying client should be replaced, and this should be
           transparent to server set participants and watchers.

        """
        all_children = []
        watcher_triggered = Event()

        def server_set_watcher(children):
            while all_children:
                all_children.pop()
            for child in children:
                all_children.append(child)
            watcher_triggered.set()

        testutil.initialize_kazoo_client_manager(ZK_HOSTS)
        client = KazooClientManager().get_client()
        server_set = ServerSet(ServerSetTestCase.SERVER_SET_PATH,
                               ZK_HOSTS,
                               waiting_in_secs=0.01)
        server_set.join(ServerSetTestCase.PORT_1, use_ip=True).join()
        server_set.monitor(server_set_watcher).join()
        watcher_triggered.wait(1)
        # Now the server set should only contain end point 1
        self.assertEqual(all_children, [ServerSetTestCase.END_POINT_1])
        watcher_triggered.clear()
        server_set.join(ServerSetTestCase.PORT_2, use_ip=True).join()
        watcher_triggered.wait(1)
        all_children.sort()
        # Now the server set should contain both end point 1 and 2
        self.assertEqual(all_children, ServerSetTestCase.END_POINTS)
        # Test recoverable failure
        client.stop()
        watcher_triggered.clear()
        client.start()
        watcher_triggered.wait(1)
        # Server set should remain the same when the client recovers
        all_children.sort()
        self.assertEqual(all_children, ServerSetTestCase.END_POINTS)
        # Test client change
        client.stop()
        watcher_triggered.clear()
        # give the monit greenlet a chance to detect failures
        gevent.sleep(1)
        # Assert the client has been replaced with a new one
        self.assertFalse(KazooClientManager().get_client() is client)
        watcher_triggered.wait(1)
        # Server set should survive the underlying client being swapped out
        all_children.sort()
        self.assertEqual(all_children, ServerSetTestCase.END_POINTS)

    @attr('destroy_serverset')
    @mock.patch("kazoo.client.KazooClient.__new__",
                new=mock.Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_serverset_destroy(self):
        testutil.initialize_kazoo_client_manager(ZK_HOSTS)
        client = KazooClientManager().get_client()
        server_set = ServerSet(ServerSetTestCase.SERVER_SET_DESTROY_PATH,
                               ZK_HOSTS,
                               waiting_in_secs=0.01)
        server_set.join(ServerSetTestCase.PORT_1, use_ip=False)
        server_set.join(ServerSetTestCase.PORT_2, use_ip=False)
        # Give time to let server set join to do its magic.
        gevent.sleep(1)
        server_set._destroy(ServerSetTestCase.END_POINT_1)
        gevent.sleep(1)
        children = client.get_children(
            ServerSetTestCase.SERVER_SET_DESTROY_PATH)
        for child in children:
            self.assertFalse(child.endswith(ServerSetTestCase.END_POINT_1))


class ServerSetWithFileTestCase(unittest.TestCase):
    """Test server set with local file."""

    SERVER_SET_PATH = "/test_server_set"
    SERVER_SET_DESTROY_PATH = "/test_server_set_destroy"
    PORT_1 = 8088
    PORT_2 = 8189
    END_POINT_1 = "%s:%d" % (socket.gethostname(), PORT_1)
    END_POINT_2 = "%s:%d" % (socket.gethostname(), PORT_2)
    END_POINTS = [END_POINT_1, END_POINT_2]

    # Initialize a singleton file watch with low wait time
    FILE_WATCH = FileWatch(polling_wait_in_seconds=0.5)
    FILE_WATCH._clear_all_watches()

    @mock.patch("kazoo.client.KazooClient.__new__",
                new=mock.Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_server_set(self):
        """Test various failure scenarios on server set implementation.

        1. When a new server joins the set, the watcher should be notified.
           In practice there is a daemon monitoring the server set change in
           zk and update the local file.
        2. When the underlying zk client disconnects and then recovers,
           the server set should be transparent to server set participants
           and watchers.
        3. When the underlying zk client messes up beyond recovery,
           it should be transparent to server set participants and watchers.

        Although when a local file is being watched, now all the code paths
        about the above behaviors got affected, we still want to test all the
        scenarios to make sure nothing breaks when a file is used.

        NOTE: to simulate the behavior in practice, when a server joins or
        leaves, we assume that there is a daemon to make the corresponding
        change to the local file.
        """
        fd, tmp_file = tempfile.mkstemp()
        all_children = []
        watcher_triggered = Event()

        def server_set_watcher(children):
            while all_children:
                all_children.pop()
            for child in children:
                all_children.append(child)
            watcher_triggered.set()

        testutil.initialize_kazoo_client_manager(ZK_HOSTS)
        client = KazooClientManager().get_client()
        server_set = ServerSet(ServerSetWithFileTestCase.SERVER_SET_PATH,
                               ZK_HOSTS,
                               waiting_in_secs=0.01,
                               file_path=tmp_file)
        server_set.join(ServerSetWithFileTestCase.PORT_1, use_ip=False).join()
        # update the local file manually here, suppose there is a daemon
        with open(tmp_file, 'w') as f:
            f.write(ServerSetWithFileTestCase.END_POINT_1)
        gevent.sleep(1)
        server_set.monitor(server_set_watcher).join()
        watcher_triggered.wait(1)
        # Now the server set should only contain end point 1
        self.assertEqual(all_children, [ServerSetWithFileTestCase.END_POINT_1])
        watcher_triggered.clear()
        server_set.join(ServerSetWithFileTestCase.PORT_2, use_ip=False).join()
        # update the local file manually here, suppose there is a daemon
        with open(tmp_file, 'w') as f:
            f.write(ServerSetWithFileTestCase.END_POINT_1 +
                    "\n" +
                    ServerSetWithFileTestCase.END_POINT_2)
        gevent.sleep(1)
        watcher_triggered.wait(1)
        all_children.sort()
        # Now the server set should contain both end point 1 and 2
        self.assertEqual(all_children, ServerSetWithFileTestCase.END_POINTS)
        # Test recoverable failure
        client.stop()
        watcher_triggered.clear()
        client.start()
        watcher_triggered.wait(1)
        # Server set should remain the same when the client recovers
        all_children.sort()
        self.assertEqual(all_children, ServerSetWithFileTestCase.END_POINTS)
        # Test client change
        client.stop()
        watcher_triggered.clear()
        # give the monit greenlet a chance to detect failures
        gevent.sleep(1)
        watcher_triggered.wait(1)
        # Server set should survive the underlying client being swapped out
        all_children.sort()
        self.assertEqual(all_children, ServerSetWithFileTestCase.END_POINTS)

        self.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)

    @attr('destroy_serverset')
    @mock.patch("kazoo.client.KazooClient.__new__",
                new=mock.Mock(side_effect=testutil.get_mock_kazoo_client))
    def test_serverset_destroy(self):
        testutil.initialize_kazoo_client_manager(ZK_HOSTS)
        client = KazooClientManager().get_client()
        client.start()
        fd, tmp_file = tempfile.mkstemp()
        server_set = ServerSet(ServerSetWithFileTestCase.SERVER_SET_DESTROY_PATH,
                               ZK_HOSTS,
                               waiting_in_secs=0.01)
        server_set.join(ServerSetWithFileTestCase.PORT_1, use_ip=False)
        server_set.join(ServerSetWithFileTestCase.PORT_2, use_ip=False)
        # update the local file manually here, suppose there is a daemon
        with open(tmp_file, 'w') as f:
            f.write(ServerSetWithFileTestCase.END_POINT_1 +
                    "\n" +
                    ServerSetWithFileTestCase.END_POINT_2)
        # Give time to let server set join to do its magic.
        gevent.sleep(1)
        server_set._destroy(ServerSetWithFileTestCase.END_POINT_1)
        # update the local file manually here, suppose there is a daemon
        with open(tmp_file, 'w') as f:
            f.write(ServerSetWithFileTestCase.END_POINT_2)
        gevent.sleep(1)
        children = client.get_children(
            ServerSetWithFileTestCase.SERVER_SET_DESTROY_PATH)
        for child in children:
            self.assertFalse(child.endswith(ServerSetWithFileTestCase.END_POINT_1))
        self.FILE_WATCH._clear_all_watches()
        os.remove(tmp_file)
