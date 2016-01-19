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
Decider is built on top of managedhashmap to be a key_value store whose keys
are strings (experiment name), values are integer ranging from 0 to 100.
This is widely used in Pinterest to decide the % / branching of code logic
and deciding experiments in real time.

Before using Decider, you need to create the ManagedData for decider, for example
using metaconfig_shell.py in examples/.

The domain of manageddata is "admin", key is "decider".

"""

import logging
import hashlib
import random


from managed_datastructures import ManagedHashMap
from ..kazoo_utils.decorators import SingletonMetaclass

log = logging.getLogger(__name__)


class Decider(object):
    __metaclass__ = SingletonMetaclass

    def __init__(self):
        self.initialized = False

    def initialize(self, zk_hosts, aws_keyfile, s3_bucket, s3_endpoint="s3.amazonaws.com"):
        self.decider_map = ManagedHashMap("admin", "decider", "Decider Values",
                                          "Map of decider name and its value",
                                          zk_hosts, aws_keyfile, s3_bucket, s3_endpoint=s3_endpoint)

    def check_initialized(self):
        if not self.initialized:
            raise Exception("Decider not initialized! Please call initialize()")

    def get_decider_value(self, experiment_name, default=0):
        """Retrieve the decider value associated with the ``experiment_name``."""
        experiment_name = experiment_name.lower()

        decider_value = self.decider_map.get(experiment_name)

        if decider_value is None:
            decider_value = default

        return decider_value

    def decide_experiment(self, experiment_name, default=False):
        """Decides if a experiment needs to be run. Calculate a random number
           between 1 and 100 and see if its less than a given value.
           Ex: decider.decide_experiment(config.decider.NEW_BOARD_PIN_TUPLES_READ)

        Args:
            default: A boolean, the default value to return if the experiment does not exist.

        """
        default_value = 100 if default else 0
        exp_value = self.get_decider_value(experiment_name, default=default_value)
        if exp_value == 100:
            return True
        if exp_value == 0:
            return False
        return random.randrange(0, 100, 1) < exp_value

    def is_id_in_experiment(self, decider_id, experiment_name):
        """Decides if an id needs to be run under an experiment_name.
           It modes the id by 100 and see whether it is within
           the decider range.
           Ex: decider.is_id_in_experiment(
               12345, config.decider.NEW_BOARD_PIN_TUPLES_READ)

        NOTE: If you are doing a user-sticky decider, you should probably use is_hashed_id_in_experiment(userid, decider)
        below. This function simply mods the userid by 100, which means that e.g. all user-ID based deciders set to 20%
        will affect the same 20% segment of users.
        """
        decider_value = self.get_decider_value(experiment_name)
        decider_id %= 100
        return decider_id < decider_value

    def is_hashed_id_in_experiment(self, unique_id, experiment_name):
        """Checks if a decider should be active given an ID (e.g. of a user, random request, etc.).

        This function computes a hash of the user ID and the decider, so different deciders will get different
        random samples of users.
           Ex: decider.is_hashed_id_in_decider(context.viewing_user.id, config.decider.NEW_BOARD_PIN_TUPLES_READ)
        """
        decider_value = self.get_decider_value(experiment_name)

        # We add the string decider_ so that if user IDs are used for unique_id, we don't have the same
        # hash as the experiment framework. That would lead to non-independence of this decider
        # and the experiment groups.
        hash_val = hashlib.md5("decider_%s%s" % (experiment_name, unique_id)).hexdigest()
        val = int(hash_val, 16) % 100

        return val < decider_value

    def set_experiment_value(self, experiment_name, value):
        """Set the value of an experiment and update in zookeeper if we are on adminapp
           Ex: decider.set_experiment_value(config.decider.NEW_BOARD_PIN_TUPLES_READ, 80)
           Args:
            experiment_name: a string, the decider's name
            value: an int, the value you want to set
            enable_audit_history: whether to save the change to audit history
            author: A string, the committer of the change.
            comment: A string, a brief description of the change

        """
        if experiment_name.lower() != experiment_name:
            raise Exception("Only use lower case for experiment names")

        if value < 0 or value > 100:
            raise Exception("Invalid experiment value")

        self.decider_map.set(experiment_name, value)

