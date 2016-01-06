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

import unittest
import gevent

from kingpin.thrift_utils.periodical import Periodical

num = 0


def f():
    global num
    num += 1


class PeriodicalTestCase(unittest.TestCase):
    def test_run_periodically(self):
        p = Periodical('f', 0, 0, f)
        gevent.sleep(0.1)

        self.assertLess(1, num)

        p.stop()
        n = num
        gevent.sleep(0.1)
        self.assertEqual(n, num)
