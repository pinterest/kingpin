#!/usr/bin/env python
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

"""Subclass of TSocket with TCP_NO_DELAY enabled."""

import socket

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TTransportException


class TNoDelaySocket(TSocket):
    """Socket implementation with TCP_NO_DELAY enabled."""

    def __init__(self, host='localhost', port=9090, unix_socket=None):
        """Initialize a TNoDelaySocket.

        Args:
            host: The host to connect to.
            port: The port to connect to.
            unix_socket: The filename of a unix socket to connect to. In this
                case, host and port will be ignored.
        """
        TSocket.__init__(self, host=host, port=port, unix_socket=unix_socket)

    def open(self):
        """Mostly copied from TSocket.open, with TCP_NODELAY on."""
        try:
            res0 = self._resolveAddr()
            for res in res0:
                self.handle = socket.socket(res[0], res[1])
                self.handle.settimeout(self._timeout)
                # turn on TCP_NODELAY
                self.handle.setsockopt(
                    socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                try:
                    self.handle.connect(res[4])
                except socket.error:
                    if res is not res0[-1]:
                        continue
                    else:
                        raise
                break
        except socket.error:
            if self._unix_socket:
                message = 'Could not connect to socket %s' % self._unix_socket
            else:
                message = 'Could not connect to %s:%d' % (self.host, self.port)
            raise TTransportException(
                type=TTransportException.NOT_OPEN, message=message)
