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

"""Request context for thrift client and server."""

from abc import ABCMeta
from abc import abstractmethod


REQUEST_TRACING_ID_KEY = "request_tracing_id"
CLIENT_ID_KEY = "client_id"


class RequestContext(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, key):
        raise NotImplementedError("Thrift RequestContext.get not implemented")

    @abstractmethod
    def set(self, key, value):
        raise NotImplementedError("Thrift RequestContext.set not implemented")


class NullRequestContext(RequestContext):
    def get(self, key):
        return None

    def set(self, key, value):
        pass
