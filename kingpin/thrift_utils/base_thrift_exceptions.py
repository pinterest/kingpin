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

"""Module that contains base Thrift exceptions.

All of our Thrift clients are decorated with a retry wrapper. In
``thrift_client_mixin.py`` if our retry failed and base connection exceptions
were raised (TTransportException, socket.error, IOError) we are repacking
them to differentiate them later. All newly repacked exceptions should
inherit from a class defined in this module.

"""


class BaseThriftError(Exception):
    """Base thrift error.

    All thrift related exceptions should inherit from this base class. That
    ensures we can catch all thrift errors in one "except..." line.

    """
    pass


class ThriftConnectionError(BaseThriftError):
    """Raised by ensure() wrapper.

    We wrap all thrift calls in ensure connection wrapper that retries to
    establish the connection. If it fails for too many times in a row because
    an exception was raised, it will be repacked in this exception and
    reraised. That way we can separately catch all thrift connection errors.

    """
    pass


class ThriftConnectionTimeoutError(ThriftConnectionError):
    """ Raised when connection establishment encounters timeout. """
    pass
