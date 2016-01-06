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

import mock

from kingpin.manageddata.decider import Decider
from unittest import TestCase
import mock_zk_config_manager

TEST_ZK_HOSTS = ['observerzookeeper010:2181']
TEST_AWS_KEYFILE = "test_keyfile"
TEST_S3_BUCKET = "test_bucket"


class DeciderTestCase(TestCase):
    experiment_name = "some_exp"
    camel_experiment_name = "Some_Exp"
    experiment_name2 = "some_exp2"

    def setUp(self):

        self.mock_zk_config_manager = mock.patch(
            "kingpin.manageddata.managed_datastructures.ZKConfigManager",
            mock_zk_config_manager.MockZkConfigManager)
        self.mock_zk_config_manager.start()

        self.decider = Decider()
        self.decider.initialize(TEST_ZK_HOSTS, TEST_AWS_KEYFILE, TEST_S3_BUCKET)
        self.decider.set_experiment_value(self.experiment_name, 30)

    def tearDown(self):
        self.mock_zk_config_manager.stop()

    @mock.patch("kingpin.manageddata.decider.random.randrange",
                new=mock.Mock(return_value=2))
    def test_decide_experiment_success(self):
        self.assertTrue(self.decider.decide_experiment(self.experiment_name))

    @mock.patch("kingpin.manageddata.decider.random.randrange",
                new=mock.Mock(return_value=45))
    def test_decide_experiment_failure(self):
        self.assertFalse(self.decider.decide_experiment(self.experiment_name))

    def test_decide_id_in_experiment_success(self):
        self.assertTrue(self.decider.is_id_in_experiment(129, self.experiment_name))

    def test_decide_id_in_experiment_failure(self):
        self.assertFalse(self.decider.is_id_in_experiment(130, self.experiment_name))

    def test_decide_hashed_id_in_experiment_success(self):
        self.decider.set_experiment_value("a", 20)
        self.decider.set_experiment_value("b", 20)

        uids = range(1000, 1500)
        decisions_a = [self.decider.is_hashed_id_in_experiment(uid, "a") for uid in uids]
        decisions_b = [self.decider.is_hashed_id_in_experiment(uid, "b") for uid in uids]

        # The distribution Bin(500,0.2) has stddev 8.94427191
        # 3 sigma / 500 = 0.053, so with probability >99%, this test should pass.
        # Since the hash function is deterministic, if it passes now, it will always pass.
        self.assertAlmostEqual(float(sum(decisions_a))/len(uids), 0.20, delta=0.05)
        self.assertAlmostEqual(float(sum(decisions_b))/len(uids), 0.20, delta=0.05)
        self.assertNotEqual(decisions_a, decisions_b)

    def test_decide_hashed_id_in_experiment_failure(self):
        self.assertFalse(self.decider.is_id_in_experiment(129, "nonexistent"))

    def test_decider_above_maximum_value(self):
        self.assertRaises(
            Exception, self.decider.set_experiment_value, self.experiment_name, 101)

    def test_decider_below_minimum_value(self):
        self.assertRaises(
            Exception, self.decider.set_experiment_value, -1)

    def test_decider_at_minimum_value(self):
        self.decider.set_experiment_value(self.experiment_name, 0)
        self.assertEqual(0, self.decider.get_decider_value(self.experiment_name))

    def test_decider_at_maximum_value(self):
        self.decider.set_experiment_value(self.experiment_name, 100)
        self.assertEqual(100, self.decider.get_decider_value(self.experiment_name))

    def test_decider_default_value(self):
        self.assertEqual(0, self.decider.get_decider_value('missing'))
        self.assertEqual(5, self.decider.get_decider_value('missing', default=5))


    def test_case_insensitivity(self):
        self.decider.set_experiment_value(self.experiment_name, 100)
        self.assertTrue(self.decider.decide_experiment(self.camel_experiment_name))

    def test_get_decider_value_insensitivity(self):
        self.decider.set_experiment_value(self.experiment_name, 100)
        self.assertTrue(100, self.decider.get_decider_value(self.camel_experiment_name))

    def test_set_insensitivity(self):
        self.assertRaises(Exception, self.decider.set_experiment_value, self.camel_experiment_name, 30)
