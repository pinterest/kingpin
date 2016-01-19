#!/usr/bin/python
#
# Copyright (c) 2016, Pinterest Inc.  All rights reserved.

from kingpin.config_utils.s3config import S3Config
from unittest import TestCase


class ReadAwsKeyFileTestCase(TestCase):

    def test_read_aws_credentials(self):
        s3config = S3Config('kingpin/tests/config_utils/test_boto_configs.conf', "s3_bucket")
        aws_access_key_id = s3config.aws_access_key_id
        aws_secret_access_key = s3config.aws_secret_access_key
        self.assertEqual(aws_access_key_id, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        self.assertEqual(aws_secret_access_key, 'abcdefghijklmnopqrstuvwxyz1234567890')

    def test_read_nonexisting_aws_key_file(self):
        s3config = S3Config('kingpin/tests/config_utils/non_exist_test_boto_configs.conf', "s3_bucket")
        aws_access_key_id = s3config.aws_access_key_id
        aws_secret_access_key = s3config.aws_secret_access_key
        self.assertEqual(aws_access_key_id, None)
        self.assertEqual(aws_secret_access_key, None)
