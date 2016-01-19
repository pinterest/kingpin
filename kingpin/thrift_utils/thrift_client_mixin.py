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

"""Utility class to wrap thrift client to ensure connection and do retries.

Sample usage:

1. First define a client class by inheriting from your thrift-service-client
AND ThriftClientMixin (make sure your thrift-service-client class is the first
base class.) You normally should also make your class a singleton to avoid
creating too many connections to your thrift servers.

      import TestService
      from kingpin.thrift_utils.base_thrift_exceptions import ThriftConnectionError
      from kingpin.thrift_utils.thrift_client_mixin import PooledThriftClientMixin

      class TestServiceConnectionException(ThriftConnectionError):
        pass

      @singleton
      class TestServiceClient(TestService.Client,
                                  ThriftClientMixin):
          def get_connection_exception_class(self):
            return TestServiceConnectionException

2. Create a object of TestServiceClient by passing a HostsProvider
object (see common/utils/hosts.py). Then you are ready to call any
thrift RPC method.

      client = TestServiceClient(
          host_provider=HostProvider(['Test1:8000', 'Test2:8001', ...])

      homefeed = client.getHomefeed(user_id, 0, 50)

      # Or you can pass a per request timeout, e.g.
      homefeed = client.getHomefeed(user_id, 0, 50, rpc_timeout_ms=1000)


NOTE: that TestServiceClient defined above is *not* greenlet-safe. If you
want a GREENLET-SAFE client, inherit from PooledThriftClientMixin instead of
ThriftClientMixin and pass pool_size when creating the client. For example:

1.
      import TestService
      from kingpin.thrift_utils.base_thrift_exceptions import ThriftConnectionError
      from kingpin.thrift_utils.thrift_client_mixin import PooledThriftClientMixin

      class TestServiceConnectionException(ThriftConnectionError):
        pass

      @singleton
      class PooledTestServiceClient(TestService.Client,
                                        PooledThriftClientMixin):
          def get_connection_exception_class(self):
            return TestServiceConnectionException


2.
    client = PooledTestServiceClient(
        host_provider=HostProvider(['Test1:8000', 'Test2:8001', ...])
        pool_size=5)

    client.ping()


Under the hood, PooledTestServiceClient maintains a pool with pool_size
number of TestServiceClient-like objects and make sure every RPC is made
exclusively using one of the clients in the pool.

"""

import datetime
import functools
from inspect import getmembers
from inspect import ismethod
import logging
import os
import random
import socket
import sys
import time

from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from thrift.Thrift import TApplicationException
from thrift.Thrift import TException
from thrift.transport.TSocket import TTransportException
from thrift.transport.TSSLSocket import TSSLSocket
from thrift.transport.TTransport import TBufferedTransport

from ..kazoo_utils.hosts import RandomHostSelector
from .base_thrift_exceptions import ThriftConnectionTimeoutError
from .connection_pool import ConnectionPool
from .retry_policy import RetryPolicy
from .TNoDelaySocket import TNoDelaySocket


DEFAULT_RETRY_POLICY = RetryPolicy()

MILLIS_PER_SEC = 1000.0

log = logging.getLogger(__name__)

NUM_REQUESTS_FOR_CONNECTION_TEARDOWN = os.environ.get(
    'THRIFT_NUM_REQUESTS_FOR_CONNECTION_TEARDOWN', 50)

SECS_FOR_CONNECTION_TEARDOWN = os.environ.get(
    'THRIFT_SECS_FOR_CONNECTION_TEARDOWN', 10)

DEFAULT_THRIFT_PROTOCOL_FACTORY = TBinaryProtocolAcceleratedFactory()

MAX_NUM_CHARS_LOGGED_PER_PARAM = 1000


def default_thrift_client_replace_if(exception):
    """Default policy on whether to replace the underlying thrift client.

    The default policy tests whether the given exception is a user defined
    thrift exception. If the given exception is a user defined thrift exception,
    it indicates the underlying connection to the thrift server is sound and can
    continue to be used.

    Args:
        exception: Exception thrown from method invocation of the thrift client.

    Return:
        True, if the underlying thrift client needs to be replaced; otherwise,
        False will be returned.

    """
    # NOTE: this is the best I can come up with to exam whether the given
    # exception is a user defined exception or not. Currently
    # TApplicationException, TProtocolException, TTransportException, and user
    # defined thrift exceptions are all subclasses of TException; however, the
    # other types of exceptions besides user defined ones don't have thrift_spec
    # attribute
    if isinstance(exception, TException):
        if (isinstance(exception, TApplicationException) or
                hasattr(exception, 'thrift_spec')):
            return False
    return True


class DummyStatsdClient:
    def __init__(self, *args, **kwargs):
        pass

    def increment(self, stats, sample_rate=1, tags={}):
        pass

    def timing(self, *args, **kwargs):
        pass

dummy_statsd = DummyStatsdClient()


class PooledThriftClientMixinMetaclass(type):
    """Metaclass for creating subclass of PooledThriftClientMixin."""

    def __new__(mcs, classname, bases, classdict):
        # We assume thrift-client class is the first base of the subclass
        # of PooledThriftClientMixin.
        _thrift_client_cls = bases[0]

        # The following two functions are meant to be injected into
        # the created subclass of PooledThriftClientMixin as its
        # member functions.
        def _ensure_use_pooled_client(self, method, timeout=5000,
                                      expiration=5000, e2e_timeout=5000):
            """Ensure a call is made exclusively on a client in the connection
            pool.

            Args:
                method: a method from a thrift-client class.
                timeout: the maximum time (ms) to wait for a connection.
                expiration: the maximum time (ms) the method may execute using
                    a connection before an exception may be raised.
                e2e_timeout: the end-to-end timeout (ms), including both the
                    time spent on acquire the connection and rpc, and all the
                    retries if there is any.

            Returns:
                a wrapped version of method that grabs a client from the pool
                and uses the client to perform the method.

            """
            @functools.wraps(method)
            def wrap_with_pool(*args, **kwargs):
                if self.client_pool.qsize() == 0:
                    log.info("Contention on thrift-pool %s. Empty pool." %
                             self.client_pool.pool_name)
                with self.client_pool.get_connection(
                        timeout=timeout / MILLIS_PER_SEC,
                        expiration=expiration / MILLIS_PER_SEC,
                        replace_if=self.conn_replace_policy,
                        e2e_timeout=e2e_timeout / MILLIS_PER_SEC) as client:
                    return method(client, *args, **kwargs)

            return wrap_with_pool

        def __init__(self, host_provider, pool_size=5, timeout=5000,
                     retry_policy=None, connection_wait_timeout=5000,
                     connection_expiration=5000, statsd_client=dummy_statsd,
                     socket_connection_timeout=None,
                     always_retry_on_new_host=False,
                     retry_count=3,
                     conn_replace_policy=default_thrift_client_replace_if,
                     protocol_factory=DEFAULT_THRIFT_PROTOCOL_FACTORY,
                     e2e_timeout=5000,
                     is_ssl=False, validate=True, ca_certs=None,
                     failed_retry_policy_log_sample_rate=1.0):
            """Constructor for PooledThriftClientMixin's subclass.

            The five timeout parameters interact in the following way:

                1. Wait 'connection_wait_timeout' to acquire a connection from
                   the pool. If timeout, give up and raise an exception (most
                   likely gevent.queue.Empty).

                2. Once a connection is acquired, the number of milliseconds
                   specified by 'connection_expiration' is available for the
                   connection, including all retries.  If more time elapses,
                   a services.utils.connection_pool.ExpiredConnection exception
                   is raised.

                3. If the underlying connection needs to be established for
                   first time, we will only wait up to
                   ``socket_connection_timeout``, if connection timeout
                   happens, we will get socket error, and the retry
                   mechanism will start to kick in. By default, it is None,
                   which will fallback to timeout specified.

                4. If the RPC fails or takes more than 'timeout' milliseconds
                   and the 'connection_expiration' limit has not been eached,
                   the RPC will be retried up to three times.

                5. ``e2e_timeout`` is the end-to-end timeout setting that take
                    both ``connection_wait_timeout`` and
                    ``connection_expiration``. If you only want to specify one
                    timeout on your client, specify this one.

            In general, the ExpiredConnection exception should not be caught in
            the context of retry logic. The exception is meant to be used as
            the last resort to prevent connection leaks caused by clients
            holding the connection forever.

            If the desired behavior is to guarantee time for all retries, the
            recommended client strategy is to set 'connection_expiration' to at
            least 'timeout' multiplied by the number of retries allowed (3).

            Args:
                host_provider: a HostProvider object to that provide a list of
                available "host:port".
                pool_size: max number of clients in the pool.
                socket_connection_timeout: the socket timeout (ms) for
                    establishing the underlying socket connection for the
                    first time.
                timeout: the socket timeout (ms) passed ot the connection pool.
                retry_policy: an instance of RetryPolicy. It should have a
                    function 'should_retry' that takes an exception as argument
                    and return a boolean value indicating whether the RPC
                    should be retried. See RetryPolicy in retry_policy.py for
                    the default retry policy.
                connection_wait_timeout: the maximum time (ms) to wait for a
                    connection to become available in the connection pool.
                connection_expiration: the maximum time (ms) a connection is
                    allowed to be out of the connection pool.
                statsd_client: a statsd client to report stats.
                socket_connection_timeout: timeout when connecting to a thrift
                    server.
                always_retry_on_new_host: whether to always retry on a new host.
                    By default, only the last retry is on a new host.
                retry_count: total number of attempts before fail a request.
                conn_replace_policy: a function that takes one argument which
                    is an exception and return a boolean indicating whether
                    to discard the connection for the exception. By default,
                    a connection is discarded for all exceptions other than user
                    defined thrift exceptions.
                protocol_factory: thrift protocol factory.
                e2e_timeout: end-to-end timeout in milliseconds. It includes
                    time taken for establishing connection, retries if
                    necessary, etc. If you only specify one timeout on your
                    client, this is the one you should specify.
                is_ssl: use SSL connection or not, default is False.
                validate: Set to False to disable SSL certificate validation.
                ca_certs: Filename to the Certificate Authority pem file.
                failed_retry_policy_log_sample_rate: sometimes the "failed retry policy
                    becomes too overwhelming so you can set a logging
                    sample rate which ranges from [0, 1.0].

            """
            # This is the mixin class for the objects we create in the pool.
            class _ThriftClientMixinClass(_thrift_client_cls,
                                          ThriftClientMixin):
                # noinspection PyMethodParameters
                def get_connection_exception_class(mixin_self):
                    return self.get_connection_exception_class()

            def _close_conn(client):
                """Close the socket maintained by the client."""
                client.teardown_connection()

            # For every method in _ThriftClientMixinClass, create a
            # corresponding method with the same name but work with the client
            # in the pool.
            for method_name, method in getmembers(_ThriftClientMixinClass,
                                                  predicate=ismethod):
                # Ignore private methods.
                if method_name[0][0] == '_':
                    continue

                # Ignore methods not in _thrift_client_cls.
                client_attr = getattr(_thrift_client_cls, method_name, None)
                if client_attr is None or not ismethod(client_attr):
                    continue

                # Create delegating method in self for method in
                # _ThriftClientMixinClass (whose methods are the same as
                # those in _thrift_client_cls.)
                #
                # Note the created self.method_name is *not* bound
                # to self when it's invoked. It's an unbound method
                # from _ThriftClientMixinClass decorated by
                # _ensure_use_pooled_client() which bounds 'method' with a
                # client from the pool.
                setattr(self, method_name,
                        self._ensure_use_pooled_client(
                            method,
                            timeout=connection_wait_timeout,
                            expiration=connection_expiration,
                            e2e_timeout=e2e_timeout))

            # Pool name
            pool_name = "%s.%s" % (_thrift_client_cls.__module__,
                                   _thrift_client_cls.__name__)
            # The pool that contains non-greenlet-safe mixin clients.
            self.client_pool = ConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                close_conn_f=_close_conn,
                conn_cls=_ThriftClientMixinClass,
                host_provider=host_provider,
                timeout=timeout,
                statsd_client=statsd_client,
                retry_policy=retry_policy,
                socket_connection_timeout=socket_connection_timeout,
                always_retry_on_new_host=always_retry_on_new_host,
                retry_count=retry_count,
                protocol_factory=protocol_factory,
                is_ssl=is_ssl,
                validate=validate,
                ca_certs=ca_certs,
                failed_retry_policy_log_sample_rate=failed_retry_policy_log_sample_rate)

            # Function that decide whether to discard a connection when
            # exception happens.
            self.conn_replace_policy = conn_replace_policy

        # Inject the two member functions in PooledThriftClientMixin's
        # subclass.
        classdict['_ensure_use_pooled_client'] = _ensure_use_pooled_client
        classdict['__init__'] = __init__

        return super(PooledThriftClientMixinMetaclass, mcs).__new__(
            mcs, classname, bases, classdict)


class PooledThriftClientMixin(object):
    """Greenlet-safe version of ThriftClientMixin.

    It wraps around ThriftClientMixin by pooling multiple ThriftClientMixin
    objects and delegates operations on them exclusively. Sample usage:

    class PooledTestServiceClient(TestService.Client,
                                      PooledThriftClientMixin):
        def get_connection_exception_class(self):
            return SomeExceptionClass

    """
    __metaclass__ = PooledThriftClientMixinMetaclass


## ---------------------------------------- ##
##   Start Definition of ThriftClientMixin  ##
## ---------------------------------------- ##


def _param_to_str(param):
    """ convert param to a string of limited number of characters. """
    param_str = "%s" % param
    if len(param_str) > MAX_NUM_CHARS_LOGGED_PER_PARAM:
        param_str = param_str[:MAX_NUM_CHARS_LOGGED_PER_PARAM] + "..."
    return param_str


def args_to_str(*args, **kwargs):
    """Return a string for the given args/kwargs."""
    try:
        handler_args = ",".join([_param_to_str(x) for x in args])
        if kwargs:
            kw_args = ",".join(
                ["%s=%s" % (k, _param_to_str(v)) for (k, v) in kwargs.iteritems()])
            handler_args = "%s,%s" % (handler_args, kw_args)
        if isinstance(handler_args, unicode):
            return handler_args.encode('utf-8', 'ignore')
        return handler_args
    except:
        return 'ERROR'


def _is_application_exception(e):
    """ check whether an exception is application specific exception. """

    # we will retry TTransportException and IOError (including
    # socket.error, socket.timeout, because socket.error and
    # socket.timeout are both subclasses of IOError since python
    # 2.3, and we are on python 2.7+)
    return (not isinstance(e, TTransportException) and
            not isinstance(e, ThriftConnectionTimeoutError) and
            not isinstance(e, IOError))


def _is_rpc_timeout(e):
    """ check whether an exception individual rpc timeout. """
    # connection caused socket timeout is being re-raised as
    # ThriftConnectionTimeoutError now
    return isinstance(e, socket.timeout)


def ensure_connection(service_name, method_name, method):
    """Ensure that client is connected before executing method.

    .. note:: Class to which this decorator is applied **must** have
       ``get_connection_exception_class()`` method that would return the
       class of the exception to be thrown when retryable connection
       exception is repacked.

    This decorator can only be applied to class methods,
    not a non-class function.

    Args:
        method_name: A string, the name of the method to be ensured.
        method: A string, the actual method to be ensured.

    Returns:
        Whatever the executed method returns.

    """
    @functools.wraps(method)
    def method_wrapper(self, *args, **kwargs):

        req_timeout_ms = kwargs.pop('rpc_timeout_ms', self.timeout)
        conn_timeout_ms = kwargs.pop('rpc_timeout_ms',
                                     self.socket_connection_timeout)
        if conn_timeout_ms is None:
            conn_timeout_ms = req_timeout_ms
        retries_left = self.retry_count
        while retries_left:
            start_time = datetime.datetime.now()
            try:
                # Ensure connection.
                try:
                    self.connect(conn_timeout_ms, req_timeout_ms)
                except socket.timeout:
                    raise ThriftConnectionTimeoutError()
                result = method(self._client, *args, **kwargs)
                time_taken = datetime.datetime.now() - start_time
                # compute time taken into milliseconds
                time_taken_ms = time_taken.total_seconds() * 1000
                self.statsd_client.timing(
                    "client.requests.{0}.{1}".format(service_name, method_name),
                    time_taken_ms, sample_rate=0.001)
                self.refresh_connection_if_needed()
                return result
            except TApplicationException as e:
                handler_args = args_to_str(*args, **kwargs)
                time_taken = datetime.datetime.now() - start_time
                # compute time taken into milliseconds
                time_taken_ms = time_taken.total_seconds() * 1000
                log.info(
                    "Thrift call failed TApplicationException : %s(%s) : "
                    "%s:%d : time_taken_ms : %s : %s" % (
                        method_name, handler_args, self.host,
                        self.port, time_taken_ms, e))
                raise
            except Exception as e:
                t, v, tb = sys.exc_info()
                retries_left -= 1
                handler_args = args_to_str(*args, **kwargs)
                time_taken = datetime.datetime.now() - start_time
                # compute time taken into milliseconds
                time_taken_ms = time_taken.total_seconds() * 1000

                # application exception, if it is retriable as determined by
                # RetryPolicy then we simply raise the exception, no connection
                # teardown is needed, because the exception was thrown by the
                # server and transported back to the client.
                if _is_application_exception(e):
                    retry_policy_to_apply = self.retry_policy
                    if not retry_policy_to_apply:
                        retry_policy_to_apply = DEFAULT_RETRY_POLICY
                    if not retry_policy_to_apply.should_retry(e):
                        if random.random() < self.failed_retry_policy_log_sample_rate:
                            # Sample logging in case logging is too overwhelming.
                            log.info(
                                "Thrift call failed retry policy : %s(%s) :"
                                "%s:%d : time_taken_ms : %s : %s" % (
                                    method_name, handler_args, self.host,
                                    self.port, time_taken_ms, e))
                            # raise exception to stop it from being retried
                        raise t, v, tb
                elif _is_rpc_timeout(e):
                    # rpc socket timeout, not connection socket timeout
                    log.info(
                        "Thrift call failed rpc timeout : %s(%s) :"
                        "%s:%d : time_taken_ms : %s : %s" % (
                            method_name, handler_args, self.host,
                            self.port, time_taken_ms, e))
                    self.statsd_client.increment(
                        "errors.thriftclient.RpcTimeoutError",
                        sample_rate=0.01,
                        tags={'client': self.host})
                    # socket timeout, only reliable way to recover is to tear
                    # down the connection, it is probably good to select a
                    # new host, regardless whether we should retry this request
                    # or not.
                    self.teardown_connection(select_new_host=True)
                    # TODO(Yongsheng): temporarily disable this feature, we need
                    # a way to gauge the server healthiness before we can bring
                    # this feature back.
                    # raise exception to keep it from being retried.
                    # raise self.get_connection_exception_class()(e)
                else:
                    # at this point, we assume it is connectivity issue,
                    # socket read/write errors, or failing to establish
                    # connection, we will need to tear down the connection
                    # and re-establish it for subsequent calls
                    log.info(
                        "Thrift client connection fail : %s(%s) : %s:%d : "
                        "retries_left=%d : time_taken_ms : %s  %r",
                        method_name, handler_args, self.host,
                        self.port, retries_left, time_taken_ms, e)
                    self.statsd_client.increment(
                        "errors.thriftclient.ConnectionError",
                        sample_rate=0.01,
                        tags={'client': self.host})
                    # By default, for the first two retries, try the same host
                    # to rule out intermittent connectivity issue. For the last
                    # retry select a new host randomly.
                    # If ``always_retry_on_new_host`` is set True, always retry
                    # on a new host.
                    if self.always_retry_on_new_host or retries_left == 1:
                        # turn this on when we are ready to penalize bad hosts
                        # self._host_selector.invalidate()
                        self.teardown_connection(select_new_host=True)
                    else:
                        self.teardown_connection(select_new_host=False)

                # Retriable errors, but no retries left, bail.
                if not retries_left:
                    log.info(
                        "Thrift call failed all retries : %s(%s) : "
                        "%s:%d : time_taken_ms: %s %s" % (
                            method_name, handler_args, self.host, self.port,
                            time_taken_ms, e))
                    self.statsd_client.increment(
                        "errors.thriftclient.AllConnectionError",
                        sample_rate=0.01,
                        tags={'client': self.host})
                    # Repack the message and raise as a different exception.
                    raise self.get_connection_exception_class()(e), None, tb

    return method_wrapper


class ThriftClientConnectionEnsured(type):
    """Metaclass to wrap all methods with ensure_connection()."""

    def __new__(cls, classname, bases, classdict):
        _client_cls = bases[0]  # FIXME: Might not always be the first base.

        def __init__(self, host_provider, timeout=5000,
                     retry_policy=None, statsd_client=dummy_statsd,
                     socket_connection_timeout=None,
                     always_retry_on_new_host=False,
                     retry_count=3,
                     protocol_factory=DEFAULT_THRIFT_PROTOCOL_FACTORY,
                     is_ssl=False, validate=True, ca_certs=None,
                     failed_retry_policy_log_sample_rate=1.0):
            """Replacement of subclass constructor.

            Args:
                host_provider: A ``HostProvider`` to provide the list of
                    live hosts for the client to talk to.
                timeout: Timeout in milliseconds for connections.
                retry_policy: On what exception the request should be retried.
                socket_connection_timeout: timeout for establishing the
                    underlying socket connection for the first time. By
                    default, it is None, which will fallback to timeout
                    specified.
                always_retry_on_new_host: If set, always retry on a new host.
                retry_count: total number of tries of rpc.
                protocol_factory: the factory of the underlying protocol to
                                  be used for rpc.
                is_ssl: use SSL connection or not, default is False.
                validate: Set to False to disable SSL certificate validation.
                ca_certs: Filename to the Certificate Authority pem file.
                failed_retry_policy_log_sample_rate: sometimes the "failed retry policy
                becomes too overwhelming so you can set a logging
                 sample rate which ranges from [0, 1.0].
            """
            # Use expire_time of 0 for all thrift services, because the client
            # controls when to use a different endpoint.
            # Use retry_time of 0 because some requests are intrinsically
            # slow, we don't want to penalize the servers for this.
            self._host_selector = RandomHostSelector(
                host_provider, expire_time=0, retry_time=0)
            host_port_pair = self._host_selector.get_host()
            host, port = host_port_pair.split(":")
            self.host = host
            self.port = int(port)
            self.timeout = timeout
            self.is_ssl = is_ssl
            self.validate = validate
            self.ca_certs = ca_certs
            self.retry_policy = retry_policy
            self.retry_count = retry_count
            self.socket_connection_timeout = socket_connection_timeout
            self.always_retry_on_new_host = always_retry_on_new_host

            self.connected = False
            self.connected_at = 0
            self.requests_served = 0
            self.statsd_client = statsd_client
            self.protocol_factory = protocol_factory
            self.failed_retry_policy_log_sample_rate = failed_retry_policy_log_sample_rate

        classdict['__init__'] = __init__

        service_name = "Base"
        if _client_cls.__module__:
            service_name = _client_cls.__module__
            if service_name.split('.') > 0:
                service_name = service_name.split('.')[-1]

        # Wrap all methods.
        for method_name, method in getmembers(_client_cls, predicate=ismethod):
            # Ignore private methods.
            if method_name[0][0] == '_':
                continue
            # Create wrapper.
            classdict[method_name] = ensure_connection(
                service_name, method_name, method)

        return type.__new__(cls, classname, bases, classdict)


class ThriftClientMixin(object):
    """Mixin for all Thrift clients.

    Provides the following features:

      - Simplified instantiation using host and port
      - Opaque singleton instantiation
      - Lazy connection
      - Ensures client is connected on each call

    """

    __metaclass__ = ThriftClientConnectionEnsured

    def connect(self, conn_timeout_ms, req_timeout_ms):
        """Connect to the endpoint specified in self.host and self.port.

        .. note:: It makes connection only if it's not already connected.

        """
        if self.connected:
            self._socket.setTimeout(req_timeout_ms)
            return

        # Socket.
        if self.is_ssl:
            self._socket = TSSLSocket(self.host, self.port, self.validate, self.ca_certs)
        else:
            self._socket = TNoDelaySocket(self.host, self.port)
        # Set socket timeout
        self._socket.setTimeout(conn_timeout_ms)

        # Transport.
        self._transport = TBufferedTransport(self._socket)

        # open() may throw TTransportException() if fail to connect.
        self._transport.open()

        # Protocol.
        self._protocol = self.protocol_factory.getProtocol(self._transport)

        # Client.
        # Need to get the parent class of the client class
        # Ex: <class
        # 'data_clients.follower_service_client.FollowerServiceClient'>
        #  =>  <class services.follower.thrift_libs.FollowerService.Client>.
        self._client = self.__class__.__bases__[0](self._protocol)
        self.connected = True
        self.connected_at = time.time()
        self._socket.setTimeout(req_timeout_ms)

    def teardown_connection(self, select_new_host=False):
        """Tear down the connection.

        Args:
            select_new_host: Boolean, whether to reset self.host and
            self.port to a random one from host_provider specified in
            the constructor.

        """
        if self.connected:
            # Close transport, which closes its socket.
            self._transport.close()

        # Set connected to false such that next connect() call will
        # re-establish connection.
        self.connected = False
        self.connected_at = 0
        self._transport = None
        self._socket = None
        self.requests_served = 0

        if select_new_host:
            new_host, new_port = self._host_selector.get_host().split(":")
            self.host = new_host
            self.port = int(new_port)

    def _is_connection_old(self):
        """Check whether the current connection age is beyond configured time.
        """
        if not self.connected:
            return False

        return time.time() - self.connected_at > SECS_FOR_CONNECTION_TEARDOWN

    def refresh_connection_if_needed(
            self, num_requests=NUM_REQUESTS_FOR_CONNECTION_TEARDOWN):
        """Track whether we need connect to a new endpoint.

        :param num_requests: number of requests needed for connection to be
            torn down and a new endpoint chosen.  New endpoint is lazily
            established in ``ensure_connection()`` call.
        """
        self.requests_served += 1
        if (self._is_connection_old() or self.requests_served ==
                NUM_REQUESTS_FOR_CONNECTION_TEARDOWN):
            self.teardown_connection(select_new_host=True)
