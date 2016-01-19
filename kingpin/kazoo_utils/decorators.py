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


"""Common decorators.

This module contains useful decorators for Pinterest code. From
increasing code security by converting constants only holding classes to
named tuples, to function call logging and sporadic function execution.

"""

import logging
import threading

log = logging.getLogger(__name__)


class SingletonMetaclass(type):
    """Singleton class that ensures there is only one instance of the object.

    Instead of using ``singleton`` decorator use this metaclass. This ensures
    that classes remain classes as opposed to
    functions that return classes, as that is the approach the decorator takes.

    For more details consult
    http://stackoverflow.com/questions/8563130/python-singleton-class.

    To make your class singleton add ``__metaclass__``::

        class Highlander(object)
            '''There can be only one!'''

            __metaclass__ = SingletonMetaclass

            def __init__(self):
                self.x = 1

    """
    def __init__(cls, *args, **kwargs):
        super(SingletonMetaclass, cls).__init__(*args, **kwargs)
        cls.__instance = None
        cls.__lock = threading.RLock()

    def __call__(cls, *args, **kwargs):
        # Make this thread safe.
        with cls.__lock:
            if cls.__instance is None:
                cls.__instance = super(SingletonMetaclass, cls).__call__(
                    *args, **kwargs)
        return cls.__instance
