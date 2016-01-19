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

from setuptools import setup, find_packages

readme = open('README.md').read()

__VERSION__ = '1.0'

setup(
    name="kingpin",
    version=__VERSION__,
    description="kingpin",
    long_description=readme,
    scripts=['kingpin/zk_update_monitor/zk_download_data.py',
             'kingpin/zk_update_monitor/zk_update_monitor.py',
             'kingpin/zk_update_monitor/zk_util.py',
             'kingpin/zk_register/zk_register.py'],
    packages=find_packages(exclude=["kingpin.tests.*"]),
    package_dir={'kingpin': 'kingpin'},
    zip_safe = False
)