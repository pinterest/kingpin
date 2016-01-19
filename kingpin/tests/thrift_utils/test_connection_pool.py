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

import time
import unittest

import gevent
import mock

from kingpin.thrift_utils.connection_pool import ConnectionPool, ExpiredConnection


def close_conn(conn):
    pass


class MyConnection(object):
    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db


class MyBadConnection(object):
    def __init__(self, host, port, db):
        raise Exception('Failed to create Connection')


class MyBlockingConnection(object):
    def __init__(self, host, port, db):
        gevent.sleep(10)


class ConnectionPoolTestCase(unittest.TestCase):
    def test_get_put(self):
        pool = ConnectionPool('mypool', 2, close_conn, MyConnection,
                              'local', 8888, db=3)

        self.assertEqual(2, pool.qsize())
        self.assertEqual(0, pool.num_connected)
        self.assertEqual(0, pool.num_in_use)

        # Get one.
        start_ts = time.time()
        conn_holder1 = pool.get()
        end_ts = time.time()
        self.assertEqual(1, pool.qsize())
        self.assertEqual(1, pool.num_connected)
        self.assertEqual(1, pool.num_in_use)
        self.assertIsNot(conn_holder1.conn, None)
        self.assertIsInstance(conn_holder1.conn, MyConnection)
        self.assertEqual('local', conn_holder1.conn.host)
        self.assertEqual(8888, conn_holder1.conn.port)
        self.assertEqual(3, conn_holder1.conn.db)
        self.assertTrue(start_ts <= conn_holder1.last_access_time <= end_ts)

        # Get another.
        conn_holder2 = pool.get()
        self.assertEqual(0, pool.qsize())
        self.assertEqual(2, pool.num_connected)
        self.assertEqual(2, pool.num_in_use)
        self.assertIsNot(conn_holder2.conn, None)
        self.assertIsInstance(conn_holder2.conn, MyConnection)
        self.assertEqual('local', conn_holder2.conn.host)
        self.assertEqual(8888, conn_holder2.conn.port)
        self.assertEqual(3, conn_holder2.conn.db)

        self.assertIsNot(conn_holder1, conn_holder2)
        self.assertIsNot(conn_holder1.conn, conn_holder2.conn)

        # Put both back.
        pool.put(conn_holder1)
        self.assertEqual(1, pool.qsize())
        self.assertEqual(2, pool.num_connected)
        self.assertEqual(1, pool.num_in_use)
        pool.put(conn_holder2)
        self.assertEqual(2, pool.qsize())
        self.assertEqual(2, pool.num_connected)
        self.assertEqual(0, pool.num_in_use)

        # LIFO
        conn_holder3 = pool.get()
        self.assertEqual(1, pool.qsize())
        self.assertEqual(2, pool.num_connected)
        self.assertEqual(1, pool.num_in_use)
        self.assertIs(conn_holder3, conn_holder2)
        self.assertIs(conn_holder3.conn, conn_holder2.conn)
        # remember its conn for later use.
        conn_3 = conn_holder3.conn

        conn_holder4 = pool.get()
        self.assertEqual(0, pool.qsize())
        self.assertEqual(2, pool.num_connected)
        self.assertEqual(2, pool.num_in_use)
        self.assertIs(conn_holder4, conn_holder1)
        self.assertIs(conn_holder4.conn, conn_holder1.conn)

        # Replace one.
        pool.put(conn_holder3, replace=True)
        self.assertEqual(1, pool.qsize())
        self.assertEqual(1, pool.num_connected)
        self.assertEqual(1, pool.num_in_use)

        pool.put(conn_holder4)
        self.assertEqual(2, pool.qsize())
        self.assertEqual(1, pool.num_connected)
        self.assertEqual(0, pool.num_in_use)

        # Get them again.
        conn_holder5 = pool.get()
        self.assertEqual(1, pool.qsize())
        self.assertEqual(1, pool.num_connected)
        self.assertEqual(1, pool.num_in_use)
        self.assertIs(conn_holder5, conn_holder4)
        self.assertIs(conn_holder5.conn, conn_holder4.conn)

        # This is the replacement of conn_3.
        conn_holder6 = pool.get()
        self.assertEqual(0, pool.qsize())
        self.assertEqual(2, pool.num_connected)
        self.assertEqual(2, pool.num_in_use)
        # Connetion holder is the same.
        self.assertIs(conn_holder6, conn_holder3)
        # Connection held is different.
        self.assertIsNot(conn_holder6.conn, conn_3)

    def test_block(self):
        pool = ConnectionPool('mypool', 1, close_conn, MyConnection,
                              'local', 8888, db=3)
        conn_holder1 = pool.get()

        self.assertRaises(gevent.queue.Empty, pool.get, block=True, timeout=0.1)

        # Spawn a greenlet to release the conn.
        gevent.spawn(pool.put, conn_holder1)

        conn_holder3 = pool.get()
        self.assertEquals(conn_holder3, conn_holder1)

    def test_block_create_conn(self):
        pool = ConnectionPool('mypool', 10, close_conn, MyBlockingConnection,
                              'local', 8888, db=3)

        self.assertRaises(gevent.queue.Empty, pool.get, block=True, timeout=0.1)

    def test_context_manager(self):
        pool = ConnectionPool('mypool', 2, close_conn, MyConnection,
                              'local', 8888, db=3)
        with pool.get_connection() as conn:
            self.assertIsNot(conn, None)
            self.assertIsInstance(conn, MyConnection)
            self.assertEqual('local', conn.host)
            self.assertEqual(8888, conn.port)
            self.assertEqual(3, conn.db)
            self.assertEqual(1, pool.qsize())
            self.assertEqual(1, pool.num_connected)
            self.assertEqual(1, pool.num_in_use)
        self.assertEqual(2, pool.qsize())
        self.assertEqual(1, pool.num_connected)
        self.assertEqual(0, pool.num_in_use)

    def test_context_manager_exception(self):
        close_f = mock.Mock()
        pool = ConnectionPool('mypool', 2, close_f, MyConnection,
                              'local', 8888, db=3)

        e = Exception("error")
        try:
            with pool.get_connection() as conn:
                self.assertEqual(1, pool.qsize())
                self.assertEqual(1, pool.num_connected)
                self.assertEqual(1, pool.num_in_use)
                raise e
        except Exception, ex:
            self.assertIs(ex, e)

        self.assertEqual(2, pool.qsize())
        self.assertEqual(0, pool.num_connected)
        self.assertEqual(0, pool.num_in_use)
        close_f.assert_called_once_with(conn)

    def test_context_manager_exception_with_replace_if(self):
        close_f = mock.Mock()
        pool = ConnectionPool('mypool', 2, close_f, MyConnection,
                              'local', 8888, db=3)

        e = Exception("error")

        def replace_if(ex):
            return ex is not e

        try:
            with pool.get_connection(replace_if=replace_if) as conn:
                self.assertEqual(1, pool.qsize())
                self.assertEqual(1, pool.num_connected)
                self.assertEqual(1, pool.num_in_use)
                raise e
        except Exception, ex:
            self.assertIs(ex, e)

        self.assertEqual(2, pool.qsize())
        self.assertEqual(1, pool.num_connected)
        self.assertEqual(0, pool.num_in_use)

        e2 = Exception("error")
        try:
            with pool.get_connection(replace_if=replace_if) as conn:
                self.assertEqual(1, pool.qsize())
                self.assertEqual(1, pool.num_connected)
                self.assertEqual(1, pool.num_in_use)
                raise e2
        except Exception, ex:
            self.assertIs(ex, e2)

        self.assertEqual(2, pool.qsize())
        self.assertEqual(0, pool.num_connected)
        self.assertEqual(0, pool.num_in_use)
        close_f.assert_called_once_with(conn)

    def test_failed_connection_creation(self):
        pool = ConnectionPool('mypool', 1, close_conn, MyBadConnection,
                              'local', 8888, db=3)

        self.assertEqual(1, pool.qsize())
        with self.assertRaises(Exception) as cm1:
            pool.get()
        self.assertEqual('Failed to create Connection', str(cm1.exception))
        # Make sure the connection is returned to the pool.
        self.assertEqual(1, pool.qsize())

        with self.assertRaises(Exception) as cm2:
            with pool.get_connection():
                pass
        self.assertEqual('Failed to create Connection', str(cm2.exception))
        self.assertEqual(1, pool.qsize())

    def test_get_connection_expired(self):
        close_f = mock.Mock()
        pool = ConnectionPool('mypool', 1, close_f, MyConnection,
                              'local', 8888, db=3)
        with self.assertRaises(ExpiredConnection) as cm:
            with pool.get_connection(expiration=0.01) as conn:
                gevent.sleep(1)
        self.assertEqual('Connection is out pool (mypool,local,8888,db=3) for '
                         'too long (0.01 secs)', str(cm.exception))
        self.assertEqual(1, pool.qsize())
        self.assertEqual(0, pool.num_in_use)
        self.assertEqual(0, pool.num_connected)
        close_f.assert_called_once_with(conn)

        with pool.get_connection(block=False) as conn:
            self.assertIsNot(conn, None)

    def test_gc_unused_connection(self):
        close_f = mock.Mock()
        pool = ConnectionPool('mypool', 2, close_f, MyConnection,
                              'local', 8888, db=3)
        # Both connections out the pool.
        with pool.get_connection() as conn1:
            with pool.get_connection() as conn2:
                pass

        self.assertEqual(2, pool.qsize())
        self.assertEqual(0, pool.num_in_use)
        self.assertEqual(2, pool.num_connected)

        with pool.get_connection() as conn3:
            self.assertIs(conn3, conn1)
            self.assertEqual(1, pool.qsize())
            self.assertEqual(1, pool.num_in_use)
            self.assertEqual(2, pool.num_connected)
            self.assertEqual(1, len(pool._queue.queue))

            for c in pool._queue.queue:
                c.last_access_time -= 1.1

            pool._gc_unused_conn(1)

            self.assertEqual(1, len(pool._queue.queue))
            self.assertEqual(1, pool.qsize())
            self.assertEqual(1, pool.num_in_use)
            self.assertEqual(1, pool.num_connected)

        self.assertEqual(2, pool.qsize())
        self.assertEqual(0, pool.num_in_use)
        self.assertEqual(1, pool.num_connected)
        close_f.assert_called_once_with(conn2)

    def test_get_desc(self):
        pool = ConnectionPool('mypool', 2, None, MyConnection, 'arg', 2)
        self.assertEqual('mypool,arg,2,', pool.desc)

        pool = ConnectionPool('mypool', 2, None, MyConnection, arg=2)
        self.assertEqual('mypool,,arg=2', pool.desc)

        pool = ConnectionPool('mypool', 2, None, MyConnection)
        self.assertEqual('mypool,,', pool.desc)

        pool = ConnectionPool('mypool', 2, None, MyConnection, None, arg=None)
        self.assertEqual('mypool,None,arg=None', pool.desc)
