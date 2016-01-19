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
A wrapper on top of Boto S3 interfaces to read/write s3 file for Pinterest's internal usage.
"""

from __future__ import absolute_import

import ConfigParser
import os

import boto
from boto.s3.key import Key

AWS_KEYFILE_CREDENTIALS_SECTION = 'Credentials'
AWS_KEYFILE_ACCESS_KEY_FIELD = 'aws_access_key_id'
AWS_KEYFILE_SECRET_KEY_FIELD = 'aws_secret_access_key'


class S3Config(object):
    """ This class takes a boto config file with aws keys and an S3 bucket name as arguments.
        This is used as the wrapper we read and write data to S3.
    """

    def __init__(self, aws_key_file, s3_bucket, s3_endpoint="s3.amazonaws.com"):
        self.aws_access_key_id, self.aws_secret_access_key = self._get_aws_credentials_from_file(aws_key_file)
        self.s3_bucket = s3_bucket
        self.s3_endpoint = s3_endpoint
        self._set_aws_config(aws_key_file)

    def _get_aws_credentials_from_file(self, aws_key_file):
        try:
            config = ConfigParser.ConfigParser()
            config.read(aws_key_file)
            aws_access_key = config.get(AWS_KEYFILE_CREDENTIALS_SECTION,
                                        AWS_KEYFILE_ACCESS_KEY_FIELD)
            aws_secret_key = config.get(AWS_KEYFILE_CREDENTIALS_SECTION,
                                        AWS_KEYFILE_SECRET_KEY_FIELD)
            return aws_access_key, aws_secret_key
        except Exception as e:
            return None, None

    def _set_aws_config(self, config_file):
        """ Parse an AWS credential file in boto (yaml) format:
            [Credentials]
            aws_access_key_id = [aws access key id]
            aws_secret_access_key = [aws secret key id]
        """
        config = boto.config
        if os.path.isfile(config_file):
            config.load_credential_file(config_file)

    def _get_bucket_conn(self):
        # Use named endpoint to achieve data consistency
        s3_conn = boto.connect_s3(host=self.s3_endpoint,
                                  aws_access_key_id=self.aws_access_key_id,
                                  aws_secret_access_key=self.aws_secret_access_key)
        s3_bucket = s3_conn.get_bucket(self.s3_bucket, validate=False)
        return s3_bucket

    def put_config_string(self, keyname, data):
        """ Put the config data into a keyname
            will replace . with / in the keyname so that this will happen:
            discovery.service.prod -> discovery/service/prod
        """
        keyname = keyname.replace('.', '/')
        s3_bucket = self._get_bucket_conn()
        s3_key = s3_bucket.get_key(keyname)
        if s3_key is None:
            s3_key = Key(s3_bucket, keyname)
        try:
            s3_key.set_contents_from_string(data)
        except boto.exception.S3ResponseError, err:
            return err

    def get_config_string(self, keyname):
        """ Fetch the data from an S3 config file named keyname
        """
        keyname = keyname.replace('.', '/')
        s3_bucket = self._get_bucket_conn()
        s3_key = s3_bucket.get_key(keyname)
        if s3_key is not None:
            s3_data = s3_key.get_contents_as_string()
            return s3_data
        else:
            raise ValueError("404 keyname not found")