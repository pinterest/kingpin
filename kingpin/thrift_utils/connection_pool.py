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

"""Connection pool that caches connections for reuse.
"""
from contextlib import contextmanager
import random
import time
import gevent.queue

from periodical import Periodical


# If a connection isn't used for more than 280 seconds, we'll close it.
MAX_CONN_AGE_SECS = 280

# GC job runs every 60 seconds.
GC_INTERVAL_SECS = 60

DEFAULT_E2E_TIMEOUT_SECS = 15


class ExpiredConnection(Exception):
    pass


class ConnectionHolder(object):
    def __init__(self, pool_name):
        self.pool_name = pool_name
        # The actual connection object.
        self.conn = None
        # Last time it's used.
        self.last_access_time = 0

    def set_conn(self, conn):
        self.conn = conn


class ConnectionPool(object):
    def __init__(self, pool_name, pool_size, close_conn_f, conn_cls,
                 *conn_args, **conn_kwargs):
        """Constructor.

        Args:
            pool_name: name of the pool.
            pool_size: max number of connections to create in the pool.
            close_conn_f: function to close a connection. It should take
            exactly one argument which is an object returned by conn_cls.
            conn_cls: python class or function for creating a connection.
            conn_args, conn_kwargs: arguments passed to conn_cls to
            create a connection.

        """
        self.pool_name = pool_name
        self.pool_size = pool_size
        assert close_conn_f is None or hasattr(close_conn_f, '__call__')
        self.close_conn_f = close_conn_f
        assert hasattr(conn_cls, '__call__')
        self.conn_cls = conn_cls
        self.conn_args = conn_args
        self.conn_kwargs = conn_kwargs
        # The number of connections in the pool that are ever used,
        # e.g. total unique number of connections returned by get().
        # This is the maximum number of concurrent connections ever reached.
        self.num_connected = 0

        self._queue = gevent.queue.LifoQueue(maxsize=pool_size)

        for i in xrange(0, pool_size):
            # Pre-populate the pool with connection holders.
            self._queue.put(ConnectionHolder(pool_name))

        # Run garbage collection on unused connections.
        # Randomize the GC job start time.
        start_after_secs = random.randint(0, 1000 * GC_INTERVAL_SECS) / 1000.0
        self._gc_job = Periodical("ConnPool-GC-%s" % pool_name,
                                  GC_INTERVAL_SECS, start_after_secs,
                                  self._gc_unused_conn, MAX_CONN_AGE_SECS)

        self.desc = self._get_desc()

    def __del__(self):
        """Stop _gc_job in order for this object get garbage collected.

        For lack of a better way, I resort to the evil __del__.

        """
        self._gc_job.stop()

    def _create_conn(self):
        """Create a connection and return it."""
        return self.conn_cls(*self.conn_args, **self.conn_kwargs)

    def _close_conn(self, conn_holder):
        """Close the connection in conn_holder."""
        if self.close_conn_f is not None and conn_holder.conn is not None:
            try:
                self.close_conn_f(conn_holder.conn)
            except:
                pass
        conn_holder.set_conn(None)
        self.num_connected -= 1

    def _get_desc(self):
        """Return some descriptive info for the pool."""
        args_str = ''
        if self.conn_args:
            args_str = ','.join([str(x) for x in self.conn_args])
        kwargs_str = ''
        if self.conn_kwargs:
            kwargs_str = ','.join(['%s=%s' % (k, v) for (k, v)
                                   in self.conn_kwargs.iteritems()])
        return '%s,%s,%s' % (self.pool_name, args_str, kwargs_str)

    def get(self, block=True, timeout=None):
        """Get a connection holder with connection object (conn) populated.

        Args:
            block: whether to wait if queue is empty.
            timeout: the max seconds to wait. If no connection is available
            after timeout, a gevent.queue.Empty exception is thrown.

        Returns:
            a ConnectionHolder object with conn populated.

        """
        conn_holder = self._queue.get(block, timeout)
        if conn_holder.conn is None:
            tm = None
            try:
                # In case self._create_conn() blocks, it should block for max
                # timeout seconds.
                tm = gevent.Timeout.start_new(timeout, gevent.queue.Empty)
                conn_holder.set_conn(self._create_conn())
            except:
                # If we fail to create a connection, we put conn_holder back
                # and re-raise the exception.
                conn_holder.set_conn(None)
                self.put(conn_holder)
                raise
            finally:
                if tm:
                    tm.cancel()

            self.num_connected += 1

        conn_holder.last_access_time = time.time()
        return conn_holder

    def put(self, conn_holder, replace=False):
        """Put back the conn_holder (returned by get()) in queue.

        Args:
            conn_holder: connection holder returned by get()
            replace: whether to create a new replacement for this connection.

        """
        assert self._queue.qsize() < self.pool_size
        assert conn_holder.pool_name == self.pool_name

        if replace:
            self._close_conn(conn_holder)

        self._queue.put_nowait(conn_holder)

    def _gc_unused_conn(self, age_secs):
        """Garbage collect unused connections.

        If a connection hasn't been accessed for >age_secs, close it.

        """
        current_ts = time.time()

        to_be_closed = []

        # Two passes to avoid potential racing condition due to blocking
        # close_conn().
        #
        # Note: typically close_conn() should just call socket.close() which
        # is non-blocking. This is the case for redis, memcache and thrift
        # connection. For mysql connection, it sends 5 bytes to server before
        # closing the socket. There is a tiny risk that the system runs out
        # buffer for 5 bytes and blocks the write. To address this, we first
        # scan all connections to identify those that should be gc'ed, remove
        # them from their connection holder. This pass is non-blocking so it
        # won't cause racing condition (while others are trying to grab the
        # connections holders during the blocking (yield)).
        # In the second pass, we close those connections.

        # First pass: find the unused connections and remove them from
        # connection holders.
        #
        # We access the internal storage of gevent.Queue/LifoQueue to avoid
        # pop then insert back the connections.
        for conn_holder in self._queue.queue:
            if (conn_holder.conn is not None and
                    current_ts - conn_holder.last_access_time > age_secs):
                to_be_closed.append(conn_holder.conn)
                conn_holder.set_conn(None)
                self.num_connected -= 1

        # Second pass: close the unused connections.
        if self.close_conn_f is not None:
            for conn in to_be_closed:
                try:
                    self.close_conn_f(conn)
                except:
                    pass

    def qsize(self):
        """Return the free objects in the queue."""
        return self._queue.qsize()

    @property
    def num_in_use(self):
        """Return the number of connections that are currently in use, i.e.
        out the pool.
        """
        return self.pool_size - self.qsize()

    @contextmanager
    def get_connection(self, block=True, timeout=None, expiration=15,
                       replace_if=None, e2e_timeout=DEFAULT_E2E_TIMEOUT_SECS):
        """Context manager that get a connection from the pool and return it
        to the pool after use.

        Args:
            block: whether to wait if there is no connection in the pool.
            timeout: if block=true, the max time (secs) to wait for connection.
            expiration: the max time (secs) a connection can be out of the pool
                before a ExpiredConnection exception is raised.
            replace_if: function that takes one parameter (exception) and
                returns a boolean indicating whether to discard the connection.
            e2e_timeout: the end-to-end timeout (secs), including time taken to
                establish connection, retries, etc.
        """
        assert replace_if is None or hasattr(replace_if, '__call__')
        if e2e_timeout is None or e2e_timeout < 0:
            # guard against bad setting, timeout should always be specified
            e2e_timeout = max(DEFAULT_E2E_TIMEOUT_SECS, expiration)
        start_time = time.time()
        # if connection acquisition timeout is not set, or such a timeout is
        # more than end-to-end timeout, use end-to-end timeout on connection
        # acquisition; otherwise, use connection acquisition timeout passed in.
        if timeout is None or timeout < 0:
            get_conn_timeout = e2e_timeout
        else:
            get_conn_timeout = timeout if timeout < e2e_timeout else e2e_timeout
        conn_holder = self.get(block, get_conn_timeout)
        replace = False
        tm = None
        # time left is end-to-end timeout minus time taken to acquire
        # connection; we need to use the smaller of time left and connection
        # expiration timeout as the timeout of the rest of the rpc, including
        # retries.
        time_left = e2e_timeout - time.time() + start_time
        if 0 < expiration < time_left:
            time_left = expiration
        if time_left < 0:
            # guard against negative time_left, in such a case, gevent doesn't
            # seem to timeout at all.
            time_left = 0
        try:
            tm = gevent.Timeout(
                seconds=time_left,
                exception=ExpiredConnection(
                    "Connection is out pool (%s) for too long "
                    "(%s secs)" % (self.desc, time_left)))
            tm.start()

            yield conn_holder.conn
        except BaseException as e:
            if replace_if is None:
                # NOTE: we play safe here to create a new connection for every
                # unhandled exception.
                replace = True
            else:
                replace = replace_if(e)
            raise
        finally:
            if tm is not None:
                tm.cancel()
            self.put(conn_holder, replace)
