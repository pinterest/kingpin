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

import socket
import time
import unittest
from kingpin.kazoo_utils.hosts import HostsProvider

from thrift.Thrift import TApplicationException, TException
from thrift.transport.TTransport import TTransportException
import gevent
from gevent.event import Event
import mock
from kingpin.thrift_utils.connection_pool import ExpiredConnection
import types

from kingpin.thrift_utils.base_thrift_exceptions import ThriftConnectionError
from kingpin.thrift_utils.base_thrift_exceptions import ThriftConnectionTimeoutError
from kingpin.thrift_utils import thrift_client_mixin
from kingpin.thrift_utils.thrift_client_mixin import (
    ThriftClientMixin,
    args_to_str,
    MAX_NUM_CHARS_LOGGED_PER_PARAM,
    SECS_FOR_CONNECTION_TEARDOWN,
    PooledThriftClientMixin)


class FakeThriftClient(object):
    def __init__(self, protocol):
        pass

    def method_success(self):
        return True

    def method_network_error(self):
        raise TTransportException()

    def method_io_error(self):
        raise IOError('IOError')

    def method_socket_error(self):
        raise socket.error('socket error')

    def method_rpc_timeout(self):
        raise socket.timeout('timeout')

    def method_thrift_connection_timeout_error(self):
        raise ThriftConnectionTimeoutError("thrift connection timeout error")

    def method_other_error(self):
        raise TApplicationException()


class FakeThriftClientMixin(FakeThriftClient, ThriftClientMixin):

    def get_connection_exception_class(self):
        """Return the class for retryable connection exception error.

        ensure_connection() in ThriftClientMixin expects to find this
        method that gives the class of the exception to be thrown when
        retryable connection exception is repacked.

        """
        return ThriftConnectionError


HOSTS = ['fakehost1:100', 'fakehost2:200', 'fakehost3:300']


# A mock class to make random.choice not random at all. It selects element
# sequentially.
class MockRandom(object):
    def __init__(self):
        self.random_index = 0

    def choice(self, hosts_port_pairs):
        r = hosts_port_pairs[self.random_index]
        self.random_index = (self.random_index + 1) % len(hosts_port_pairs)
        return r


@mock.patch(thrift_client_mixin.__name__ + '.TNoDelaySocket',
            new=mock.MagicMock())
@mock.patch(thrift_client_mixin.__name__ + '.TBinaryProtocolAcceleratedFactory',
            new=mock.MagicMock())
class ThriftClientMixinTestCase(unittest.TestCase):

    def setUp(self):
        self.patch_random = mock.patch(
            'random.choice', new=mock.Mock(side_effect=MockRandom().choice))
        self.patch_random.start()

    def tearDown(self):
        self.patch_random.stop()

    def test_args_to_str(self):
        self.assertEqual("", args_to_str())
        self.assertEqual("1,a,True,None,k2=2,k1=v1",
                         args_to_str(1, 'a', True, None, k1='v1', k2=2))

    def test_long_args_to_str(self):
        val = ''.join(['x' for _ in xrange(2000)])
        shortened_val = val[:MAX_NUM_CHARS_LOGGED_PER_PARAM] + "..."
        self.assertEqual("1,a,True,None,k2=2,k1=%s" % shortened_val,
                         args_to_str(1, 'a', True, None, k1=val, k2=2))

    def test_unicode_args_to_str(self):
        val_unicode = u'\u4500'
        expected_str = ("%s,k1=%s" % (val_unicode, val_unicode)).encode(
            'utf-8', 'ignore')
        self.assertEqual(expected_str,
                         args_to_str(val_unicode, k1=val_unicode))

    def test_success(self):
        client = FakeThriftClientMixin(HostsProvider(HOSTS))
        self.assertTrue(client.method_success())
        self.assertEqual(1, client.requests_served)
        self.assertEqual('fakehost1', client.host)
        self.assertEqual(100, client.port)
        self.assertTrue(client.connected)

        # call it again
        self.assertTrue(client.method_success())
        self.assertEqual(2, client.requests_served)
        self.assertEqual('fakehost1', client.host)
        self.assertEqual(100, client.port)
        self.assertTrue(client.connected)

    def _test_network_error(self,
                            client,
                            method_raise_network_error):
        self.assertTrue(client.method_success())
        self.assertEqual('fakehost1', client.host)
        self.assertEqual(100, client.port)
        self.assertTrue(client.connected)
        self.assertEqual(1, client.requests_served)

        # Calling a function to simulate network error. Note that we are
        # raising TTransportException but thrift mixin makes sure it gets
        # rewritten as ThriftConnectionError.
        self.assertRaises(ThriftConnectionError, method_raise_network_error)
        # 3 attempts should be made. The last attempt results in creating
        # a connection to a new host/port, resetting requests_served.
        self.assertEqual(0, client.requests_served)
        # the last attempt should be on a different host/port.
        self.assertEqual('fakehost2', client.host)
        self.assertEqual(200, client.port)
        self.assertFalse(client.connected)

        # A good call will connect again and succeed.
        self.assertTrue(client.method_success())
        self.assertEqual(1, client.requests_served)
        self.assertEqual('fakehost2', client.host)
        self.assertEqual(200, client.port)
        self.assertTrue(client.connected)

    def test_network_error(self):
        """ test rpc causing TTransportException. """
        client = FakeThriftClientMixin(HostsProvider(HOSTS))
        self._test_network_error(client, client.method_network_error)

    def test_io_error(self):
        """ test rpc causing IOError. """
        client = FakeThriftClientMixin(HostsProvider(HOSTS))
        self._test_network_error(client, client.method_io_error)

    def test_socket_error(self):
        """ test rpc causing socket.error. """
        client = FakeThriftClientMixin(HostsProvider(HOSTS))
        self._test_network_error(client, client.method_socket_error)

    def test_thrift_connection_timeout_error(self):
        """ test rpc causing ThriftConnectionTimeoutError. """
        client = FakeThriftClientMixin(HostsProvider(HOSTS))
        self._test_network_error(
            client, client.method_thrift_connection_timeout_error)

    def test_rpc_timeout(self):
        """ test rpc causing socket.timeout. """
        client = FakeThriftClientMixin(HostsProvider(HOSTS), retry_count=2)
        self.assertRaises(ThriftConnectionError, client.method_rpc_timeout)
        # Connection should be established with the new host, but this teardown
        # and connection re-establishment should only happen once
        # disabled for now, this requests will be retried twice as specified by
        # retry_count, and each time it will try to establish the connection on
        # a new host endpoint.
        self.assertEqual('fakehost3', client.host)
        self.assertEqual(300, client.port)
        self.assertFalse(client.connected)
        # requests_served doesn't not increase.
        self.assertEqual(0, client.requests_served)

    def test_other_error(self):
        client = FakeThriftClientMixin(HostsProvider(HOSTS))
        self.assertRaises(TApplicationException, client.method_other_error)
        # Connection should be established.
        self.assertEqual('fakehost1', client.host)
        self.assertEqual(100, client.port)
        self.assertTrue(client.connected)
        # requests_served doesn't not increase.
        self.assertEqual(0, client.requests_served)

    @mock.patch(thrift_client_mixin.__name__ +
                '.NUM_REQUESTS_FOR_CONNECTION_TEARDOWN', new=2)
    def test_teardown(self):
        client = FakeThriftClientMixin(HostsProvider(HOSTS))
        self.assertTrue(client.method_success())
        # Connection should be established on the first endpoint.
        self.assertEqual('fakehost1', client.host)
        self.assertEqual(100, client.port)
        self.assertTrue(client.connected)
        self.assertEqual(1, client.requests_served)

        # Make the second call.
        self.assertTrue(client.method_success())
        # Connection should be teared down and endpoint is reset.
        self.assertEqual('fakehost2', client.host)
        self.assertEqual(200, client.port)
        self.assertFalse(client.connected)
        self.assertEqual(0, client.requests_served)

        # Make the third call.
        self.assertTrue(client.method_success())
        # Connection should be established on the second endpoint.
        self.assertEqual('fakehost2', client.host)
        self.assertEqual(200, client.port)
        self.assertTrue(client.connected)
        self.assertEqual(1, client.requests_served)

        # Make the forth call.
        self.assertTrue(client.method_success())
        # Connection should again be teared down and endpoint is reset.
        self.assertEqual('fakehost3', client.host)
        self.assertEqual(300, client.port)
        self.assertFalse(client.connected)
        self.assertEqual(0, client.requests_served)

    def test_teardown_secs(self):
        start_ts = time.time()
        client = FakeThriftClientMixin(HostsProvider(HOSTS))
        self.assertTrue(client.method_success())
        # Connection should be established on the first endpoint.
        self.assertEqual('fakehost1', client.host)
        self.assertEqual(100, client.port)
        self.assertTrue(client.connected)
        self.assertEqual(1, client.requests_served)
        self.assertGreater(client.connected_at, start_ts)

        client.connected_at -= SECS_FOR_CONNECTION_TEARDOWN

        # Make the call.
        self.assertTrue(client.method_success())
        # Connection should be teared down.
        self.assertEqual('fakehost2', client.host)
        self.assertEqual(200, client.port)
        self.assertFalse(client.connected)
        self.assertEqual(0, client.requests_served)

        # Another call.
        start_ts = time.time()
        self.assertTrue(client.method_success())
        # Connection re-established.
        self.assertEqual('fakehost2', client.host)
        self.assertEqual(200, client.port)
        self.assertTrue(client.connected)
        self.assertEqual(1, client.requests_served)
        self.assertGreater(client.connected_at, start_ts)

    def test_timeout(self):
        client = FakeThriftClientMixin(
            HostsProvider(HOSTS), timeout=5000, socket_connection_timeout=1000)
        self.assertTrue(client.method_success())
        # in case there is no rpc_timeout, the default socket connection
        # timeout should have been 1000 ms.
        client._socket.setTimeout.assert_any_call(1000)
        # in case there is no rpc_timeout, the default request timeout
        # should have been the same as timeout specified above.
        client._socket.setTimeout.assert_any_call(5000)
        self.assertTrue(client.method_success(rpc_timeout_ms=100))
        self.assertEqual(2, client.requests_served)
        self.assertEqual('fakehost1', client.host)
        self.assertEqual(100, client.port)
        self.assertTrue(client.connected)
        self.assertEqual(client.timeout, 5000)
        client._socket.setTimeout.assert_called_with(100)

        # call it again
        self.assertTrue(client.method_success(rpc_timeout_ms=200))
        self.assertEqual(3, client.requests_served)
        self.assertEqual('fakehost1', client.host)
        self.assertEqual(100, client.port)
        self.assertTrue(client.connected)
        self.assertEqual(client.timeout, 5000)
        client._socket.setTimeout.assert_called_with(200)

    def test_multiple_mixins(self):
        class AnotherThriftClient(object):
            def __init__(self, protocol):
                pass

            def method_hello(self):
                return "hello"

        class AnotherThriftClientMixin(AnotherThriftClient, ThriftClientMixin):
            pass

        client = FakeThriftClientMixin(HostsProvider(HOSTS))
        another_client = AnotherThriftClientMixin(
            HostsProvider(['anotherhost1:1000', 'anotherhost2:2000']))

        # Call a method of the first client.
        self.assertTrue(client.method_success())
        # Connection should be established.
        self.assertEqual('fakehost1', client.host)
        self.assertEqual(100, client.port)
        self.assertTrue(client.connected)
        self.assertEqual(1, client.requests_served)

        # Call a method of the second client.
        self.assertEqual("hello", another_client.method_hello())
        # Connection should be established. Due to the way random.choice is
        # mocked, the host/port should be the second one.
        self.assertEqual('anotherhost2', another_client.host)
        self.assertEqual(2000, another_client.port)
        self.assertTrue(another_client.connected)
        self.assertEqual(1, another_client.requests_served)


class FakeThriftException(TException):
    thrift_spec = ()

    def __init__(self):
        pass

    def read(self, iprot):
        pass

    def write(self, oprot):
        pass

    def validate(self):
        pass


class AnotherFakeClient(object):
    num_calls = 0
    in_flight_calls = 0
    sleep_event = Event()

    def __init__(self, protocol):
        self.in_use = False
        pass

    def method_success(self):
        exclusive = not self.in_use
        self.in_use = True
        AnotherFakeClient.num_calls += 1
        AnotherFakeClient.in_flight_calls += 1
        num_in_flight = AnotherFakeClient.in_flight_calls
        # Yield
        gevent.sleep(0)
        AnotherFakeClient.in_flight_calls -= 1
        self.in_use = False
        return exclusive, num_in_flight

    def method_sleep(self, seconds_to_sleep):
        gevent.sleep(seconds_to_sleep)
        return True

    def method_sleep_set_event(self, seconds_to_sleep):
        AnotherFakeClient.sleep_event.set()
        # yield to make sure that the other greenlet will wait at least
        # seconds_to_sleep
        gevent.sleep(0)
        gevent.sleep(seconds_to_sleep)
        return True

    def method_network_error(self):
        raise TTransportException()

    def method_other_error(self):
        raise TApplicationException()

    def method_user_defined_error(self):
        raise FakeThriftException()


class FakePooledThriftClientMixin(AnotherFakeClient, PooledThriftClientMixin):

    def get_connection_exception_class(self):
        """Return the class for retryable connection exception error.

        ensure_connection() in ThriftClientMixin expects to find this
        method that gives the class of the exception to be thrown when
        retryable connection exception is repacked.

        """
        return ThriftConnectionError


@mock.patch(thrift_client_mixin.__name__ + '.TNoDelaySocket',
            new=mock.MagicMock())
@mock.patch(thrift_client_mixin.__name__ + '.TBinaryProtocolAcceleratedFactory',
            new=mock.MagicMock())
class PooledThriftClientMixinTestCase(unittest.TestCase):

    def test_wrap(self):
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS))
        self.assertEqual(types.FunctionType, type(client.method_success))
        self.assertEqual(types.FunctionType, type(client.method_network_error))
        self.assertEqual(types.FunctionType, type(client.method_other_error))
        self.assertEqual(types.MethodType,
                         type(client.get_connection_exception_class))
        self.assertIs(
            client.get_connection_exception_class.im_func,
            FakePooledThriftClientMixin.__dict__[
                'get_connection_exception_class'])

    def test_success(self):
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS))
        self.assertEqual((True, 1), client.method_success())

    def test_network_error(self):
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS))
        # Calling a function to simulate network error. Note that we are
        # raising TTransportException but thrift mixin makes sure it gets
        # rewritten as ThriftConnectionError.
        self.assertRaises(ThriftConnectionError, client.method_network_error)

    def test_replace_if(self):
        def replace_if(ex):
            return isinstance(ex, ThriftConnectionError)

        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             conn_replace_policy=replace_if)
        self.assertRaises(ThriftConnectionError, client.method_network_error)
        self.assertEqual(0, client.client_pool.num_connected)

        self.assertRaises(TApplicationException, client.method_other_error)
        self.assertEqual(1, client.client_pool.num_connected)

    def test_default_replace_policy(self):
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS))

        self.assertRaises(TApplicationException, client.method_other_error)
        self.assertEqual(1, client.client_pool.num_connected)

        self.assertRaises(FakeThriftException, client.method_user_defined_error)
        self.assertEqual(1, client.client_pool.num_connected)

    def _run_method_success(self, client, n):
        for i in xrange(0, n):
            exclusive, num_in_flights = client.method_success()
            self.assertTrue(exclusive)
            # pool_size (5) limits the max concurrency.
            self.assertGreaterEqual(5, num_in_flights)

    def test_concurrency(self):
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             pool_size=5)

        self.assertEqual(0, AnotherFakeClient.in_flight_calls)
        AnotherFakeClient.num_calls = 0

        greenlets = []
        for i in xrange(0, 10):
            greenlets.append(gevent.spawn(self._run_method_success,
                                          client, 3))
        gevent.joinall(greenlets)
        self.assertEqual(30, AnotherFakeClient.num_calls)

    def test_e2e_timeout(self):
        # a test case where connection expiration is not triggered, but the
        # overall time taken is longer than connection expiration timeout.
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             pool_size=1,
                                             connection_wait_timeout=300,
                                             connection_expiration=300)
        result = gevent.spawn(client.method_sleep_set_event, 0.2)
        # make sure the previous call has acquired a connection from the pool
        AnotherFakeClient.sleep_event.wait(0.1)
        AnotherFakeClient.sleep_event.clear()
        # the total time this call will wait is 0.2 + 0.2 seconds = 400ms
        # however, this won't trigger connection expiration timeout, since the
        # connection acquisition time was not counted towards connection
        # expiration timeout
        self.assertTrue(client.method_sleep(0.2))
        result.join()

        # a test case to show that end-to-end timeout is tighter than connection
        # expiration timeout, even though they are set to the same value.
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             pool_size=1,
                                             connection_wait_timeout=300,
                                             connection_expiration=300,
                                             e2e_timeout=300)
        result = gevent.spawn(client.method_sleep_set_event, 0.2)
        AnotherFakeClient.sleep_event.wait(0.1)
        AnotherFakeClient.sleep_event.clear()
        # this should raise exception, because this call will wait 400 ms
        # (200ms on connection acquisition, 200ms on execution), while the
        # end-to-end timeout is set to 300ms
        self.assertRaises(ExpiredConnection, client.method_sleep, 0.2)
        result.join()

        # a test case to show that everything works as expected even if only
        # end-to-end timeout is specified.
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             pool_size=1,
                                             e2e_timeout=300)
        result = gevent.spawn(client.method_sleep_set_event, 0.2)
        AnotherFakeClient.sleep_event.wait(0.1)
        AnotherFakeClient.sleep_event.clear()
        t0 = time.time()
        self.assertRaises(ExpiredConnection, client.method_sleep, 0.2)
        result.join()

    def test_connection_expiration(self):
        # The connection should expire immediately.
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             connection_expiration=1)
        self.assertRaises(ExpiredConnection, client.method_sleep, 0.1)

        # The expiration is long enough to allow the method to complete.
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             connection_expiration=1000)
        self.assertTrue(client.method_sleep(0.1))

    def test_connection_wait_timeout(self):
        # The connection acquisition attempt should be immediately abandoned.
        start = time.time()
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             pool_size=0,
                                             connection_wait_timeout=1)
        self.assertRaises(gevent.queue.Empty, client.method_success)
        self.assertTrue(time.time() - start < 0.2)

        # The connection acquisition attempt should wait for a short period
        # before giving up.
        start = time.time()
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             pool_size=0,
                                             connection_wait_timeout=300)
        self.assertRaises(gevent.queue.Empty, client.method_success)
        self.assertTrue(time.time() - start > 0.2)

    def test_statsd_client_is_called(self):
        """Test that a statsd client gets called.

        We'll pass the client to :class:`PooledThriftClientMixinMetaclass`.
        """
        class TestStatsdClient:
            def __init__(self, *args, **kwargs):
                self.val = 0
                self.timing_data = {}

            def increment(self, stats, sample_rate=1):
                self.val += 1

            def timing(self, key, val, **kwargs):
                self.timing_data[key] = val

        sc = TestStatsdClient()
        client = FakePooledThriftClientMixin(host_provider=HostsProvider(HOSTS),
                                             statsd_client=sc)
        client.method_success()
        self.assertTrue(
            "client.requests.test_thrift_client_mixin.method_success" in
            sc.timing_data)
