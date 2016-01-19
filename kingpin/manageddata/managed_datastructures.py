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

"""
A various of utils we use on top of config_utils which provides typical datastructures
like lists, hashmap, mappedlist and Json. The content of these datastructures is
stored in S3, and uses Zookeeper as the notification. ZK Update Monitor is used
to sync the newest content to local disks.

"""
import copy
import itertools
import logging
import random
import re

import simplejson as json

from kingpin.config_utils import ZKConfigManager
from manageddata_utils import memoized_property
from manageddata_utils import unicode_str


log = logging.getLogger(__name__)
FILE_BASED_MANAGED_LIST_KEY_FORMAT = '%s-%s'
MANAGED_DATA_CONFIG_FILE_FORMAT = '/var/config/config.manageddata.%s.%s'


class ManagedDataStructureSingletonMetaclass(type):
    """Singleton metaclass for ManagedDataStructure that ensures there is only one instance of the
    unique identifier of (list_domain, list_key).

    Since this metaclass is shared by multiple inherited ManagedDataStructure classes, it ensures
    that the instance corresponding to the unique identifier of (list_domain, list_key) can only be
    one type.

    """
    def __init__(cls, *args, **kwargs):
        super(ManagedDataStructureSingletonMetaclass, cls).__init__(*args, **kwargs)
        cls._instances = {}

    def __call__(cls, list_domain, list_key, *args, **kwargs):
        key = '%s:%s' % (list_domain, list_key)
        if key in cls._instances:
            if cls != type(cls._instances[key]):
                raise Exception('%s:%s is already in use with a different structure' % (
                    list_domain, list_key))
        else:
            cls._instances[key] = super(ManagedDataStructureSingletonMetaclass, cls).__call__(
                list_domain, list_key, *args, **kwargs)
        return cls._instances[key]


class ManagedDataStructure(object):
    """Based class for Managed Data structure.

    Managed Data Structure is parameterized singleton, which means there will only be one instance
    for each unique (list_domain, list_key). For all inherited classes, they needs to have
    ``list_domain`` and ``list_key`` arguments as the first two arguments because this is how
    ``ManagedDataStructureSingletonMetaclass`` manages singleton.

    """

    all_managed_lists = {}
    all_domains = set([])
    list_domain_to_name_mappings = {}
    domain_collapsed = {}
    hidden = {}

    _ALPHANUMERICS_RE = re.compile("^[-0-9A-Za-z_]+$")

    __metaclass__ = ManagedDataStructureSingletonMetaclass

    def __init__(self,
                 list_domain,
                 list_key,
                 list_name,
                 list_description,
                 zk_hosts,
                 aws_keyfile,
                 s3_bucket,
                 s3_endpoint="s3.amazonaws.com",
                 force_config_update=False,
                 renderer=None,
                 key_validators=None,
                 validators=None):
        """
        Args:
           ``list_domain`` - Classification of list, such as 'spam'.
           ``list_key`` - Key for list, such as 'url_whitelist'.
           ``list_name`` - Name of list, such as 'URL Whitelist'.
           ``list_description`` - Name of list, such as 'A Whitelist of URLs,
                                  blah blah blah'.
           ``renderer`` - A function that should take the list and return a
                          formatted version.
           ``validator`` - A function that takes an item and validates it
                           before adding it to the list.
            ``zk_hosts`` - a list of zookeeper endpoints
            ``aws_keyfile``: - a file containing the aws access and secret keys

        """

        if not self._ALPHANUMERICS_RE.search(list_key):
            raise ValueError("list_key must contain only alphanumerics, _ and -")

        if not self._ALPHANUMERICS_RE.search(list_domain):
            raise ValueError(
                "list_domain must contain only alphanumerics, _ and -")

        self.list_domain = list_domain
        self.list_key = list_key
        self.list_version_key = "{0}-latest_version".format(self.list_key)
        self.list_name = list_name
        self.list_description = list_description

        self.force_config_update = force_config_update
        self.renderer = renderer
        self.key_validators = key_validators or []
        self.validators = validators or []

        self.zk_hosts = zk_hosts
        self.aws_keyfile = aws_keyfile
        self.s3_bucket = s3_bucket
        self.s3_endpoint = s3_endpoint

        if self.list_domain not in self.all_managed_lists:
            self.all_managed_lists[self.list_domain] = {}

        # Make sure (list_domain, list_key) is unique among all managed data structures.
        assert self.list_key not in self.all_managed_lists[self.list_domain], \
            '%s:%s is already in use' % (self.list_domain, self.list_key)

        self.all_managed_lists[self.list_domain][self.list_key] = self
        self.all_domains.add(self.list_key)

        if self.list_domain not in self.list_domain_to_name_mappings:
            self.list_domain_to_name_mappings[self.list_domain] = (
                self.list_domain.upper())

    @classmethod
    def add_list_domain_to_name_mapping(cls, list_domain, domain_name,
                                        collapsed=False, hidden=False):
        """Add a domain. If collapsed, show the domain as a link in the UI
           rather than in the front page.

        """
        if not cls._ALPHANUMERICS_RE.search(list_domain):
            raise ValueError(
                "list_domain must contain only alphanumerics and _")

        cls.list_domain_to_name_mappings[list_domain] = domain_name
        cls.domain_collapsed[list_domain] = collapsed
        cls.hidden[list_domain] = hidden

    # App Interactions #

    def validate(self, value):
        if not self.validators:
            return value
        for validator in self.validators:
            out = validator(value)
            if out is not None:
                return out

    def validate_key(self, key):
        if not self.key_validators:
            return key
        for validator in self.key_validators:
            out = validator(key)
            if out is not None:
                return out
        return None

    def get_rendered_list(self):
        raise Exception("Must be implemented by sub-class")

    # Management Interactions #

    def get_list_domain(self):
        return self.list_domain

    def get_list_key(self):
        return self.list_key

    def get_list_name(self):
        return self.list_name

    def get_list_description(self):
        return self.list_description

    def get_list_input_description(self):
        if self.validators:
            return ', '.join(v.__doc__ for v in self.validators)
        return self.list_description

    # Interaction with all lists #

    @classmethod
    def get_managed_list(cls, domain, key):
        return cls.all_managed_lists.get(domain, {}).get(key)

    @classmethod
    def list_all_managed_lists(cls, domain):
        return cls.all_managed_lists.get(domain, {}).values()

    @classmethod
    def is_list_domain_collapsed(cls, domain):
        return cls.domain_collapsed.get(domain)

    @classmethod
    def is_list_domain_hidden(cls, domain):
        return cls.hidden.get(domain)

    @classmethod
    def get_list_domains(cls):
        return cls.list_domain_to_name_mappings

    @memoized_property
    def local_config_file_path(self):
        return MANAGED_DATA_CONFIG_FILE_FORMAT % (self.list_domain.lower(), self.list_key.lower())

    @memoized_property
    def zk_config_manager(self):
        config_file_path = self.local_config_file_path
        return ZKConfigManager(self.zk_hosts, self.aws_keyfile, self.s3_bucket,
                               config_file_path, self._read_config_callback,
                               s3_endpoint=self.s3_endpoint)

    def _read_config_callback(self):
        raise NotImplementedError("Config based datastructures should override _read_config_callback method")


class ManagedList(ManagedDataStructure):
    def __init__(self,
                 list_domain,
                 list_key,
                 list_name,
                 list_description,
                 zk_hosts,
                 aws_keyfile,
                 s3_bucket,
                 s3_endpoint="s3.amazonaws.com",
                 update_callback=None,
                 data_type=None, **kwargs):
        """
        Args:
           list_domain: Classification of list, such as 'spam'.
           list_key: Key for list, such as 'url_whitelist'.
           list_name: Name of list, such as 'URL Whitelist'.
           list_description: Name of list, such as 'A Whitelist of URLs,
                             blah blah blah'.
           update_callback: Callback when data is updated.
           data_type: If specified, values in this list will be enforced to
                            the given type. This arg should be passed by type
                            name directly -- without quotes.

        Note: If this managed list uses config backend. Please make sure to do
        the puppet change to add it to zk_update_monitor config.
        """
        super(ManagedList, self).__init__(
            list_domain, list_key, list_name, list_description, zk_hosts, aws_keyfile, s3_bucket,
            s3_endpoint=s3_endpoint, **kwargs)
        self.update_callback = update_callback
        self.data_type = data_type

    def add(self, value):
        """Add the value to the list. Values already in the list will not be added.

        Args:
            value: a string.
        Returns the number of values added.

        """
        value = self.validate(value)
        if value is None:
            log.error('None value is not allowed')
            return 0

        if self.data_type is not None:
            value = self.data_type(value)

        self._reload_config_data()
        if self.contains(value):
            return 0
        if self._update_config(self._get_list() + [value]):
            return 1
        return 0

    def add_many(self, values):
        """Add many values to the list. Values already in the list will not be added.

        Args:
            values: A list of strings.
        Returns:
            Number of values added.

        """
        valid_values = filter(lambda v: v is not None, map(lambda v: self.validate(v), values))
        if not valid_values:
            return 0

        if self.data_type is not None:
            valid_values = map(self.data_type, valid_values)


        self._reload_config_data()
        values_to_add = filter(lambda v: not self.contains(v), valid_values)
        if not values_to_add:
            return 0
        if self._update_config(self._get_list() + values_to_add):
            return len(values_to_add)
        return 0

    def remove(self, value):
        """Remove the value from the list.

        Returns the number of values removed.

        """
        if self.data_type is not None:
            value = self.data_type(value)

        self._reload_config_data()
        if not self.contains(value):
            return 0
        new_list = self._get_list()[:]
        new_list.remove(value)
        if self._update_config(new_list):
            return 1
        return 0

    def set_list(self, new_list):
        if self.data_type is not None:
            new_list = map(self.data_type, new_list)
        return self._update_config(new_list)

    def get_list(self, start_idx=0, end_idx=-1):
        return self._get_list()[start_idx:] if end_idx == -1 else \
            self._get_list()[start_idx:end_idx + 1]

    def get_shuffled_list(self):
        l = self.get_list()
        random.shuffle(l)
        return l

    def get_set(self):
        return self._get_set()

    def contains(self, value):
        return value in self._get_set()

    def delete(self):
        return self._update_config([])

    def clear(self):
        """This method only deletes self.list_key, while keeps self.list_version_key.

        Note: This method is used by admin category whitelist web tool, which
        does many delete and add operations.

        For config based managed list, this method is equal to ``delete``.

        """
        return self.delete()

    def count(self):
        return len(self._get_list())

    def get_rendered_list(self):
        items = self.get_list()
        if self.renderer:
            rendered_items = self.renderer(items)
        else:
            rendered_items = items

        # Convert all final keys and values to unicode for display.
        encoder = lambda s: s.encode('utf-8') if isinstance(s, basestring) else str(s)
        keys = itertools.imap(unicode_str, itertools.imap(encoder, items))
        vals = itertools.imap(unicode_str, itertools.imap(encoder, rendered_items))

        return zip(keys, vals)

    # CONFIG BASED MANAGED LIST OPERATIONS #

    def _get_list(self):
        if not hasattr(self, '_list'):
            self._reload_config_data()
        return self._list

    def _get_set(self):
        if not hasattr(self, '_set'):
            self._reload_config_data()
        return self._set

    def _read_config_callback(self, data):
        """Callback function when the ZKConfigManager reads new config data.

        Args:
            data: A string, the new data in the config file.

        """
        # In case of corrupted data.
        try:
            decoded_data = json.loads(data)
            if isinstance(decoded_data, list):
                self._list = decoded_data
            else:
                log.error("Loaded data: bad format, expecting a list")
                self._list = []
        except Exception:
            log.error("Unable to load data, exception encountered")
            self._list = []
        self._set = set(self._list)

        if self.update_callback:
            self.update_callback()

    def _update_config(self, new_list):
        """Update the config with the new list.

        Note: If you want to replace the managed list with a new list. Instead of calling
        ``delete()`` and ``add()`` multiple times, this is the recommended way to do that since it
        only updates the data in zk once.

        Args:
            new_list: A list.
        Returns:
            True if succeeded, otherwise False.

        """
        if self.data_type is not None:
            invalid_values = [v for v in new_list if type(v) is not self.data_type]
            if invalid_values:
                raise ValueError("new_list contains invalid values: [%s]" % str(invalid_values))

        return self.zk_config_manager.update_zk(
            json.dumps(self._get_list()), json.dumps(new_list), self.force_config_update)

    def _reload_config_data(self):
        """Reload the data from config file into ``self._list`` and ``self._set``.

        Note: When changing the managed list using add() and remove() from command line, the
        DataWatcher's greenlet does not work, you need to call this explicitly to update the list
        so as to make following changes.

        """
        try:
            self.zk_config_manager.reload_config_data()
        except IOError:
            log.info('Error reading config file in managed list %s:%s' % (
                self.list_domain, self.list_key))
            # Assume there is empty data in the config file.
            self._read_config_callback('')


class ManagedHashMap(ManagedDataStructure):
    def __init__(self, list_domain, list_key, list_name, list_description,
                 zk_hosts, aws_keyfile, s3_bucket, s3_endpoint="s3.amazonaws.com",
                 key_type=None, value_type=None, **kwargs):
        super(ManagedHashMap, self).__init__(
            list_domain, list_key, list_name, list_description, zk_hosts,
            aws_keyfile, s3_bucket, s3_endpoint=s3_endpoint, **kwargs)
        self.key_type = key_type
        self.value_type = value_type

    def set(self, key, value):
        # Added checking for key.
        key = self.validate_key(key)
        if key is None:
            log.error('not setting the key:%s as key failed validation' % key)
            return 0
        value = self.validate(value)
        if value is None:
            log.error('not setting value for the key:%s as value failed validation' % key)
            return 0

        key, value = self._convert_key_value(key, value)

        self._reload_config_data()
        if key not in self._get_dict() or self._get_dict()[key] != value:
            new_dict = dict(self._get_dict())
            new_dict[key] = value
            if self._update_config(new_dict):
                return 1
        return 0

    def set_many(self, dict_to_add):
        self._reload_config_data()
        tmp_dict = {}
        for k, v in dict_to_add.items():
            if self.validate(v) is not None and (k not in self._get_dict() or v != self._get_dict()[k]):
                tmp_dict[k] = v
        tmp_dict = self._convert_dict(tmp_dict)
        new_dict = self._get_dict().copy()
        new_dict.update(tmp_dict)
        if tmp_dict:
            if self._update_config(new_dict):
                return len(tmp_dict)
        return 0

    def set_map(self, dict_to_replace):
        """Replace the current map with the new one(with invalid values filtered out).

        Note: This is only available for config backed managed hashmap.

        Returns:
            The number of key/values in the new map.

        """

        self._reload_config_data()
        filtered_dict = {k: v for k, v in dict_to_replace.iteritems()
                         if self.validate(v) is not None}
        converted_dict = self._convert_dict(filtered_dict)
        if self._update_config(converted_dict):
            return len(converted_dict)
        return 0

    def remove(self, key):
        if self.key_type is not None:
            key = self.key_type(key)
        self._reload_config_data()
        if key in self._get_dict():
            new_dict = dict(self._get_dict())
            del new_dict[key]
            if self._update_config(new_dict):
                return 1
        return 0

    def get(self, key):
        return self._get_dict().get(key)

    def get_many(self, keys):
        tmp_dict = {}
        for k in keys:
            if k in self._get_dict():
                tmp_dict[k] = self._get_dict()[k]
        return tmp_dict

    def get_keys(self):
        return self._get_dict().keys()

    def get_all(self):
        return self._get_dict().copy()

    def delete(self):
        self._update_config({})
        return

    def contains(self, key):
        return key in self._get_dict()

    def get_rendered_map(self):
        items = self.get_all()
        if self.renderer:
            rendered = self.renderer(items.values(), as_map=True)
        else:
            rendered = {}

        rendered_out = {}
        for key, value in items.iteritems():
            if type(value) is list:
                value = str(value)
            else:
                value = str(value.encode('utf-8'))
            u_key = unicode(key, 'utf-8') if not isinstance(key, unicode) else key
            u_val = unicode(value, 'utf-8') if not isinstance(value, unicode) else value
            rendered_val = rendered.get(value) if rendered else value
            u_rendered = (unicode(rendered_val, 'utf-8')
                          if not isinstance(rendered_val, unicode) else rendered_val)
            rendered_out[u_key] = (u_val, u_rendered)
        return rendered_out

    # CONFIG BASED MANAGED MAP OPERATIONS #
    def _convert_key_value(self, k, v):
        if self.key_type is not None:
            k = self.key_type(k)
        if self.value_type is not None:
            v = self.value_type(v)
        return k, v

    def _convert_dict(self, old_dict):
        new_dict = {}
        for k, v in old_dict.items():
            k, v = self._convert_key_value(k, v)
            new_dict[k] = v
        return new_dict

    def _get_dict(self):
        if not hasattr(self, '_dict'):
            self._reload_config_data()
        return self._dict

    def _read_config_callback(self, data):
        """Callback function when the ZKConfigManager reads new config data.

        Args:
            data: A string, the new data in the config file.

        """
        # In case of corrupted data.
        try:
            decoded_data = json.loads(data)
            if type(decoded_data) is dict:
                valid_data = {}
                # After dumping a dict to json and then loading it back,
                # all keys in the original dict will be converted to str
                # type, regardless what original types they have. We shall
                # convert keys back to certain type if it is specified.
                try:
                    for k, v in decoded_data.items():
                        if self.key_type is not None:
                            k = self.key_type(k)
                        valid_data[k] = v
                except ValueError:
                    log.error("Loaded dict contains key(s) that are not able to be converted to the original type.")
                    valid_data = {}
                self._dict = valid_data
            else:
                log.error("Loaded data: bad format, expecting a dict")
                self._dict = {}
        except Exception:
            log.error("Unable to load data, exception encountered")
            self._dict = {}

    def _update_config(self, new_dict):
        """Update the config with the new dictionary.

        Args:
            new_dict: A dictionary.

        Returns:
            True if succeeded, otherwise False.

        """

        invalid_dict = {}
        for k, v in new_dict.items():
            if (self.key_type is not None and type(k) is not self.key_type) or \
                    (self.value_type is not None and type(v) is not self.value_type):
                invalid_dict[k] = v
        if invalid_dict:
            raise ValueError("new dict contains invalid items: %s" % str(invalid_dict))

        return self.zk_config_manager.update_zk(json.dumps(self._get_dict(), sort_keys=True),
                                                json.dumps(new_dict, sort_keys=True),
                                                self.force_config_update)

    def _reload_config_data(self):
        """Reload the data from config file into ``self._dict``

        Note: When changing the managed list using add() and remove() from command line, the
        DataWatcher's greenlet does not work, you need to call this explicitly to update the list
        so as to make following changes.

        """
        try:
            self.zk_config_manager.reload_config_data()
        except IOError:
            log.info('Error reading config file in managed map %s:%s' % (
                self.list_domain, self.list_key))
            # Assume there is empty data in the config file.
            self._read_config_callback('')


class ManagedMappedList(ManagedDataStructure):
    """Managed mapped list - Provides access to add/remove/show a key=>list mapping

    Please note given the way we serialize the payload, the key of a dict SHOULD ALWAYS BE STRING.

    """
    def __init__(self, list_domain, list_key,
                 list_name, list_description, zk_hosts, aws_keyfile, s3_bucket,
                 s3_endpoint="s3.amazonaws.com", dedup=True, update_callback=None):
        super(ManagedMappedList, self).__init__(list_domain, list_key,
                                                list_name, list_description,
                                                zk_hosts, aws_keyfile, s3_bucket,
                                                s3_endpoint=s3_endpoint)
        self.update_callback = update_callback if hasattr(update_callback, '__call__') else None
        self.dedup = dedup

    def set_list(self, key, the_list):
        """Set a list for the key.

        Args:
            key: The key for a given list
            the_list: The list will be set in

        Returns:
            True indicates that the result is as same as caller thought (dict value is set,
            and the change will be propagated to other servers if there's any).
            False if there's an error, or say the original dict changed during caller's operation.

        """
        self._reload_config_data()
        new_dict = copy.deepcopy(self._dict)
        if self.dedup:
            the_list = list(set(the_list))
        new_dict[key] = the_list
        return self._update_config(new_dict)

    def set_lists(self, key_to_list_dict):
        self._reload_config_data()
        new_dict = copy.deepcopy(self._dict)
        if self.dedup:
            key_to_list_dict = {key: list(set(value))
                                for key, value in key_to_list_dict.iteritems()}
        new_dict.update(key_to_list_dict)
        return self._update_config(new_dict)

    def add_item(self, key, item):
        """Add an item into the list for a given key

        Args:
            key: The key for a given list
            item: The item will be set in

        Returns:
            True indicates that the result is as same as caller thought (dict value is set,
            and the change will be propagated to other servers if there's any).
            False if there's an error, or say the original dict changed during caller's operation.

        """
        self._reload_config_data()
        new_dict = copy.deepcopy(self._dict)
        if key not in new_dict:
            new_dict[key] = []
        if self.dedup and (item in self._dict_set.get(key, set())):
            return True
        new_dict[key].append(item)
        return self._update_config(new_dict)

    def add_items(self, key, items):
        """Add items into the list for a given key

        Args:
            key: The key for a given list
            items: A list of items which will be set in

        Returns:
            True indicates that the result is as same as caller thought (dict value is set,
            and the change will be propagated to other servers if there's any).
            False if there's an error, or say the original dict changed during caller's operation.

        """
        self._reload_config_data()
        new_dict = copy.deepcopy(self._dict)
        if key not in new_dict:
            new_dict[key] = []
        if self.dedup:
            items = set(items)
            items = [item for item in items if item not in self._dict_set.get(key, set())]
        if not items:
            return True
        new_dict[key].extend(items)
        return self._update_config(new_dict)

    def get_all(self):
        """Get the whole dictionary

        Returns:
            A dict represents this resource.

        """
        return self._get_dict()

    def get_list(self, key):
        """Get the list with a given key

        Args:
            key: The key for a given list

        Returns:
            A list for a given key.  If the key doesn't exist, it returns []

        """
        return self._get_dict().get(key, [])

    def contain_item(self, key, item):
        """See if the given key contains a certain item

        Args:
            key: The key for a given list
            item: The item which will be looked for

        Returns:
            True if the item exists, False otherwise

        """
        _dict_set = self._get_dict_set()
        if key not in _dict_set:
            return False
        return item in _dict_set[key]

    def contain_items(self, key, items):
        """See if the given key contains a list items

        Args:
            key: The key for a given list
            items: A list of items which will be looked for

        Returns:
            A list of found items.  [] if none of them exists

        """
        _dict_set = self._get_dict_set()
        if key not in _dict_set:
            return []
        return [item for item in items if item in _dict_set[key]]

    def remove_list(self, key):
        """Remove the list for a given key

        Args:
            key: The key for a given list

        Returns:
            True indicates that the result is as same as caller thought (dict value is set,
            and the change will be propagated to other servers if there's any).
            False if there's an error, or say the original dict changed during caller's operation.

        """
        self._reload_config_data()
        if key not in self._dict:
            return True
        new_dict = copy.deepcopy(self._dict)
        del new_dict[key]
        return self._update_config(new_dict)

    def remove_item(self, key, item):
        """Remove an item for a given key

        Args:
            key: The key for a given list
            item: The item will be removed

        Returns:
            True indicates that the result is as same as caller thought (dict value is set,
            and the change will be propagated to other servers if there's any).
            False if there's an error, or say the original dict changed during caller's operation.

        """
        self._reload_config_data()
        if key not in self._dict:
            return True
        if item not in self._dict_set[key]:
            return True
        new_dict = copy.deepcopy(self._dict)
        if self.dedup:
            new_dict[key] = [val for val in self._dict[key] if val != item]
        else:
            new_dict[key].remove(item)
        return self._update_config(new_dict)

    def remove_items(self, key, items):
        """Remove items for a given key

        Args:
            key: The key for a given list
            items: A list of items that will be removed

        Returns:
            True indicates that the result is as same as caller thought (dict value is set,
            and the change will be propagated to other servers if there's any).
            False if there's an error, or say the original dict changed during caller's operation.

        """
        self._reload_config_data()
        if key not in self._dict:
            return True
        items = [item for item in items if item in self._dict_set[key]]
        if not items:
            return True
        new_dict = copy.deepcopy(self._dict)
        if self.dedup:
            new_dict[key] = [val for val in self._dict[key] if val not in items]
        else:
            for item in items:
                new_dict[key].remove(item)
        return self._update_config(new_dict)

    def clear(self, force_update=False):
        return self._update_config({}, force_update=force_update)

    def _get_dict(self):
        if not hasattr(self, '_dict'):
            self._reload_config_data()
        return self._dict

    def _get_dict_set(self):
        if not hasattr(self, '_dict_set'):
            self._dict_set = {key: set(val) for key, val in self._get_dict().iteritems()}
        return self._dict_set

    def _read_config_callback(self, data):
        """Callback function when the ZKConfigManager reads new config data.

        Args:
            data: A string, the new data in the config file.

        """
        # In case of corrupted data.
        try:
            decoded_data = json.loads(data)
            if type(decoded_data) is dict:
                self._dict = decoded_data
            else:
                self._dict = {}
        except Exception:
            self._dict = {}
        self._dict_set = {key: set(val) for key, val in self._dict.iteritems()}
        if self.update_callback:
            self.update_callback()

    def _update_config(self, new_dict, force_update=False):
        """Update the config with the new dictionary.

        Args:
            new_dict: A dictionary.

        Returns:
            True if succeeded, otherwise False.

        """
        return self.zk_config_manager.update_zk(
            json.dumps(self._get_dict(), sort_keys=True),
            json.dumps(new_dict, sort_keys=True),
            force_update=force_update)

    def _reload_config_data(self):
        """Reload the data from config file into ``self._dict``

        Note: When changing the managed mapped list using add() and remove() from command line, the
        DataWatcher's greenlet does not work, you need to call this explicitly to update the list
        so as to make following changes.

        """
        try:
            self.zk_config_manager.reload_config_data()
        except IOError:
            log.info('Error reading config file in managed mapped list %s:%s' % (
                self.list_domain, self.list_key))
            # Assume there is empty data in the config file.
            self._read_config_callback('{}')


class ManagedJsonConfig(ManagedDataStructure):
    """ Managed data structure for plain json config in string.
    """
    def __init__(self, list_domain, list_key, list_name, list_description, zk_hosts, aws_keyfile, s3_bucket,
                 s3_endpoint="s3.amazonaws.com", update_callback=None, **kwargs):
        super(ManagedJsonConfig, self).__init__(
            list_domain, list_key, list_name, list_description, zk_hosts,
            aws_keyfile, s3_bucket, s3_endpoint=s3_endpoint,  **kwargs)

        self.set_update_callback(update_callback)

    def get_json_config(self):
        """ Get the json config in string
        """
        if not hasattr(self, '_json_config'):
            self._reload_config_data()
        return self._json_config

    def set_json_config(self, new_value):
        """ Update the managed json config with a new string

        Args:
            new_value: The new json config in string

        Returns:
            True if update succeeds, False otherwise
        """
        if new_value is None:
            log.error("Invalid new config being none")
            return False

        return self.zk_config_manager.update_zk(
            self.get_json_config(), new_value, self.force_config_update)

    def _read_config_callback(self, value):
        self._json_config = value

        if self.update_callback:
            self.update_callback()

    def set_update_callback(self, update_callback):
        self.update_callback = update_callback if callable(update_callback) else None

    def _reload_config_data(self):
        """Reload the data from config file into 'self._json_config'

        Note: When changing the managed json config using set_json_config() from command line, the
        DataWatcher's greenlet does not work, you need to call this explicitly to update the config
        so as to make following changes.
        """
        try:
            self.zk_config_manager.reload_config_data()
        except IOError:
            log.info('Error reading config file in managed json config %s:%s' % (
                self.list_domain, self.list_key))
            # Assume there is empty data in the config file.
            self._read_config_callback('')


class ManagedJsonSerializableDataConfig(ManagedDataStructure):
    """Managed data structure for arbitrary JSON-serializable data."""

    def __init__(self, list_domain, list_key, list_name, list_description,
                 zk_hosts, aws_keyfile, s3_bucket, s3_endpoint="s3.amazonaws.com",
                 encoder_cls=json.JSONEncoder, decoder_cls=json.JSONDecoder,
                 update_callback=None, force_config_update=None):

        kwargs = {}
        if force_config_update is not None:
            kwargs['force_config_update'] = force_config_update

        super(ManagedJsonSerializableDataConfig, self).__init__(
            list_domain, list_key, list_name, list_description, zk_hosts,
            aws_keyfile, s3_bucket, s3_endpoint=s3_endpoint, **kwargs)

        self.encoder_cls = encoder_cls
        self.decoder_cls = decoder_cls
        self.update_callback = None
        if update_callback:
            self.set_update_callback(update_callback)

    def get_data(self):
        if not hasattr(self, '_data'):
            self._reload_config_data()
        return self._data

    def set_data(self, new_data):
        """Serialize and persist new data to ZK.

        Args:
            new_value: The new json config
        Returns:
            True if update succeeds, False otherwise

        """
        try:
            old_data = self.get_data()
            serialized_data = json.dumps(old_data, cls=self.encoder_cls, sort_keys=True) if old_data else ''
            serialized_new_data = json.dumps(new_data, cls=self.encoder_cls, sort_keys=True) if new_data else ''
        except TypeError:
            log.error("Error JSON-serializing data for managed data config")
            log.error(self.get_data())
            log.error(new_data)
            return False

        return self.zk_config_manager.update_zk(
            serialized_data, serialized_new_data, self.force_config_update)

    def _read_config_callback(self, new_data):
        self._data = self.decoder_cls().decode(new_data) if new_data else None

        if self.update_callback:
            self.update_callback()

    def set_update_callback(self, update_callback):
        self.update_callback = update_callback if callable(update_callback) else None

    def _reload_config_data(self):
        """Reload (and deserialize) data from the config file into 'self._data'.

        Note: When changing the config using self.set_data() from the command
        line, the DataWatcher's greenlet does not work, so you need to call
        this method explicitly to update the config. (Note copied from
        ManagedJsonConfig:_reload_config_data).

        """
        try:
            self.zk_config_manager.reload_config_data()
        except IOError:
            log.info('Error reading config file in managed json config %s:%s' % (
                self.list_domain, self.list_key))
            # Assume there is empty data in the config file.
            self._read_config_callback('')

    def clear(self):
        return self.zk_config_manager.update_zk(None, '', True)
