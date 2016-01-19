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


# Sentinel object for fast dictionary access.
_missing = object()


class memoized_property(property):
    """A decorator that converts a class method into a memoizing property."""

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func

    def __set__(self, obj, value):
        obj.__dict__[self.__name__] = value

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        value = obj.__dict__.get(self.__name__, _missing)
        if value is _missing:
            value = self.func(obj)
            obj.__dict__[self.__name__] = value
        return value

    def __delete__(self, obj):
        obj.__dict__.pop(self.__name__, None)


def unicode_str(content, encoding=None, errors=None):
    """Try to decode ``content``` into unicode given an
    ``encoding``. By default it will try to decode using utf-8.

    If the input is already a unicode string, it will be returned
    unchanged.

    Args:
        ``content``: a basestring.
        ``encoding``: an encoding to use to decode ``content``.
        ``errors``: (optional) specifies the response when the input
           string can't be converted according to the encoding.
           Legal values for this argument:
               'strict': Return None. This is the default behavior.
               'replace': replace with U+FFFD, 'REPLACEMENT CHARACTER'
               'ignore': just leave the character out of the result

    Returns:
        Unicode representation of ``content``.
    """
    if not encoding or encoding == 'undefined':
        encoding = 'utf-8'
    errors = errors or 'strict'
    try:
        if not isinstance(content, unicode):
            content = unicode(content, encoding, errors=errors)
    except (UnicodeDecodeError, LookupError, TypeError):
        content = None
    return content
