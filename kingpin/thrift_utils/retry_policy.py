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

"""The policy on whether to retry a thrift request on a given exception."""

from .ttypes import RetriableRPCException


class RetryPolicy(object):
    """This class encapsulate the logic on whether to retry a thrift request
     given a particular exception.

    """
    def should_retry(self, exception):
        """Whether to retry a thrift request on the given exception.

        Args:
            exception: The exception thrown by the previous thrift request.

        Returns:
            True, if the request should be retried; False, otherwise.

        """
        # only service specific exceptions need to be considered
        return isinstance(exception, RetriableRPCException)
