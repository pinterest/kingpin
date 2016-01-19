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

"""Utility to run a function periodically.

Sample Usage:
    def f():
        print "I'm running"

    p = Periodical('my_cron', 60, 0, f)

f() will be called once every 60 seconds. The first run starts
now (after 0 seconds).

To stop it, call:
    p.stop()

IMPORTANT: you must call stop() in order for the periodical to get garbage
collected.

"""

import logging
import time

import gevent

log = logging.getLogger(__name__)


class Periodical(object):
    def __init__(self, name, interval_secs, start_after_secs,
                 f, *args, **kwargs):
        """Constructor.

        Args:
            name: name of the periodical for bookkeeping.
            interval_secs: frequency to run.
            start_after_secs: first run starts after start_after_secs.
            f: function to run.
            args: args passed to f
            kwargs: kwargs passed to f

        """
        self.name = name
        self.interval_secs = interval_secs

        self.f = f
        self.args = args
        self.kwargs = kwargs

        self.last_timestamp = time.time() + start_after_secs - interval_secs
        self.greenlet = gevent.spawn(self._run)

    def _run(self):
        """Run the wrapped function periodically"""
        try:
            while True:
                ts = time.time()
                if self.last_timestamp + self.interval_secs <= ts:
                    self.last_timestamp = ts
                    try:
                        self.f(*self.args, **self.kwargs)
                    except gevent.GreenletExit:
                        # We are notified to exit.
                        raise
                    except BaseException as e:
                        # We ignore other exceptions.
                        log.error("Exception %s caught in Periodical %s " % (
                            repr(e), self.name))

                # sleep until the time for the next run.
                sleep_secs = self.last_timestamp + self.interval_secs \
                    - time.time()

                if sleep_secs < 0:
                    sleep_secs = 0

                gevent.sleep(sleep_secs)

        except gevent.GreenletExit:
            log.info("Periodical %s stopped." % self.name)

    def stop(self):
        """Stop the periodical.

        It must be called in order to garbage collect this object.

        """
        self.greenlet.kill(block=True)
