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


"""Tests for common decorators."""
import unittest

from kingpin.kazoo_utils import decorators


class SingletonsTestCase(unittest.TestCase):

    def test_singleton_metaclass(self):
        """Test singleton metaclass."""

        class Foo(object):
            """Metaclass enforced singleton."""

            __metaclass__ = decorators.SingletonMetaclass

            def __init__(self):
                self.state = None

        foo = Foo()
        # Not set yet.
        self.assertIs(foo.state, None)
        # Set and check if it was set.
        foo.state = 1
        self.assertEquals(foo.state, 1)

        # Now create another object.
        bar = Foo()
        # If it is a singleton it should be the same as foo.
        self.assertIs(foo, bar)
        # Since it is singleton it should have inherited the state,
        # and is not set to None.
        self.assertIsNot(bar.state, None)
        self.assertEquals(bar.state, 1)
