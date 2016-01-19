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

import logging
import simplejson

from kingpin.kazoo_utils import KazooClientManager
from kingpin.config_utils import ZKBaseConfigManager

log = logging.getLogger(__name__)

ZK_DOWNLOAD_DATA_PREFIX = None
METACONFIG_ZK_PATH_FORMAT = '/metaconfig/metaconfig/{}'
DEPENDENCY_ZK_PATH_FORMAT = '/metaconfig/dependency/{}'
ZK_DOWNLOAD_DATA_SERVERSET_FORMAT = 'zk_download_data.py -f /var/serverset/{} -p {} -m serverset'
ZK_DOWNLOAD_DATA_CONFIGV2_FORMAT = 'zk_download_data.py -f /var/config/{} --from-s3 {} -m config -p {}'


class MetaConfigManager(object):
    """
    A manager which handles all dependencies and manageddata / serversets via MetaConfigs.

    MetaConfig: Configurations which tell zk_update_monitor how to update / download serverset or configurations.
    One MetaConfig example (in json):
         [
            {
                "config_section_name": "config.manageddata.spam.test_grey_list",
                "zk_path": "/config/manageddata/spam/test_grey_list",
                "command": "zk_download_data.py -f /var/config/config.manageddata.spam.test_grey_list --from-s3 /data/config/manageddata/spam/test_grey_list -m config -p /config/manageddata/spam/test_grey_list",
                "type": "config",
                "max_wait_in_secs": 0
            }
         ]

    The manager has the following capabilities:
    1. Create / update a MetaConfig and archive the history to S3 using ZKConfigManager.
    2. Create a new dependency ZK Node
    3. Add a metaconfig to existing dependency
    4. Some templating : Auto-generating the Zk_download_data command for serverset / configv1 / configv2
    5. Convert a .conf metaconfig file into json formatted metaconfig

    """

    def __init__(self, zk_hosts, aws_keyfile, s3_bucket, s3_endpoint="s3.amazonaws.com"):
        """
        To initialize the MetaConfigManager.

        Args:
            zk_hosts: The zookeeper hosts that MetaConfig manager need to put dependencies to.
            aws_keyfile: The aws key file which contains aws access and secret keys.
            s3_bucket: the s3 bucket that stores the metaconfigs.
            s3_endpoint: the s3 endpoint which stores the metaconfigs.
        """
        self.zk_hosts = zk_hosts
        self.aws_keyfile = aws_keyfile
        self.s3_bucket = s3_bucket
        self.s3_endpoint = s3_endpoint

        self._kazoo_client().ensure_path("/metaconfig/metaconfig")
        self._kazoo_client().ensure_path("/metaconfig/dependency")

    def _kazoo_client(self):
        # Make the connection timeout long so the executive shell creating the
        # metaconfigs will not timeout.
        kazoo_client = KazooClientManager(self.zk_hosts,
                                          start_timeout=200.0,
                                          session_timeout=200.0).get_client()
        if not kazoo_client:
            KazooClientManager(self.zk_hosts)._reconnect()
            kazoo_client = KazooClientManager(self.zk_hosts).get_client()
        return kazoo_client

    def _construct_absolute_dependency_leaf_node(self, source, destination):
        destination_zk_path = DEPENDENCY_ZK_PATH_FORMAT.format(destination)
        return "{}/{}".format(destination_zk_path, source)

    def get_metaconfig_content(self, metaconfig_name):
        """
        Get the JSON format of MetaConfig from S3.

        Args:
            metaconfig_name: the metaconfig's name.

        Returns:
            the content of metaconfig in json string
        """
        metaconfig_zk_path = METACONFIG_ZK_PATH_FORMAT.format(metaconfig_name)
        if not self._kazoo_client().exists(metaconfig_zk_path):
            return ""
        return ZKBaseConfigManager(self.zk_hosts, metaconfig_zk_path,
                                   self.aws_keyfile, self.s3_bucket,
                                   s3_endpoint=self.s3_endpoint).get_data()

    def add_to_dependency(self, destination_dependency, new_member):
        """
        Add a metaconfig / dependency to another dependency.
        The metaconfig / dependency should already exist.

        Args:
            destination_dependency:
                    The destination dependnecy to be added to.
                    Should be always been ended with ".dep"
            new_member: the source dependency / metaconfig to add to destination_dependency.
                        If a dependency, should end with .dep

        Returns:
            Throwing exception if the operation is failed.
        """
        destination_zk_path = DEPENDENCY_ZK_PATH_FORMAT.format(destination_dependency)
        self._kazoo_client().ensure_path(destination_zk_path)
        # Ensure source is a valid existing metaconfig / dependency
        if new_member.endswith(".dep"):
            source_zk_path = DEPENDENCY_ZK_PATH_FORMAT.format(new_member)
        else:
            source_zk_path = METACONFIG_ZK_PATH_FORMAT.format(new_member)
        if not self._kazoo_client().exists(source_zk_path):
            raise Exception("The source MetaConfig/Dep does not exist")
        znode_path_added_to_dependency = self._construct_absolute_dependency_leaf_node(new_member,
                                                                                       destination_dependency)
        if self._kazoo_client().exists(znode_path_added_to_dependency):
            raise Exception("The dependency has already been added")
        # Create a children node to that dependency node
        try:
            returned_path = self._kazoo_client().create(znode_path_added_to_dependency)
            if returned_path == znode_path_added_to_dependency:
                return
            else:
                raise Exception("Failed to add dependency because the returned path doesn't equal to desired path."
                                "Desired: %s, Actual Returned: %s", znode_path_added_to_dependency, returned_path)
        except Exception as e:
            raise Exception("Failed to add dependency: %s", e)

    def create_dependency(self, dependency_name):
        """
        Create a new dependency for metaconfigs.

        Args:
            dependency_name: must ending with ".dep"

        return:
            Throwing exception is the operation is failed
        """
        if not dependency_name.endswith(".dep"):
            raise Exception("The dpendency name should be ended with '.dep'")

        zk_path = DEPENDENCY_ZK_PATH_FORMAT.format(dependency_name)
        self._kazoo_client().ensure_path(zk_path)

    def dependency_exists(self, dependency_name):
        """
        Check if dependency already exists.

        Args:
            dependency_name: must ending with ".dep"

        Returns:
            True if exist, false if not
        """
        dependency_zk_path = DEPENDENCY_ZK_PATH_FORMAT.format(dependency_name)
        if self._kazoo_client().exists(dependency_zk_path):
            return True
        else:
            return False

    def is_member_of_dependency(self, dependency_name, metaconfig_name):
        """
        Check if the metaconfig is already been added to the dependency.

        Args:
            dependency_name: the dependency name
            metaconfig_name: the metaconfig name

        Returns:
            True is already added, false if not.
        """
        znode_path_added_to_dependency = self._construct_absolute_dependency_leaf_node(metaconfig_name, dependency_name)
        if self._kazoo_client().exists(znode_path_added_to_dependency):
            return True
        else:
            return False

    def remove_dependency(self, destination_dependency, member_to_remove):
        """
        Remove a metaconfig / dependency from another dependency.

        Args:
            destination_dependency:
                    The destination dependnecy to be removed from.
                    Should be always been ended with ".dep"
            member_to_remove:
                    the member under that dependency to remove.

        Returns:
            Throwing exception if the operation is failed.
        """
        destination_zk_path = DEPENDENCY_ZK_PATH_FORMAT.format(destination_dependency)
        if not self._kazoo_client().exists(destination_zk_path):
            raise Exception("The dependency to delete from doesn't exist")

        znode_path_deleted_from_dependency = self._construct_absolute_dependency_leaf_node(
            member_to_remove, destination_dependency)
        if not self._kazoo_client().exists(znode_path_deleted_from_dependency):
            raise Exception("The dependency relation doesn't exist")

        try:
            self._kazoo_client().delete(znode_path_deleted_from_dependency)
        except Exception as e:
            raise Exception("Failed to remove dependency: %s", e)

    def get_dependency_members(self, dependency_name):
        """
        Get a list of names of dependency members of a dependency

        Args:
            dependency_name: the name of dependency (should end with .dep)

        Returns:
            a list of dependency member names
        """
        dependency_path = DEPENDENCY_ZK_PATH_FORMAT.format(dependency_name)
        return self._kazoo_client().get_children(dependency_path)

    def update_metaconfig(self, name, content):
        """
        Put Metaconfig in S3 and create correcponding ZK nodes.
        We will reuse the logic in config_utils to guarantee the writes to both S3 and ZkNode.
        In this case, because we don't sync MetaConfig to local files, we will always turn force_update to True
        so the change will be populated to Zookeeper and S3.
        We will also enable audit history for all changes related to MetaConfig contents.

        If the MetaConfig is not in ZK, we will foce create it, if it is in ZK, we will update it.
        Args:
            name: the name of config. For example, discovery.serverset.prod
            content: the content of the MetaConfig, should be in JSON format.

        Returns:
            True if successfully updated, otherwise execption is thrown.
        """
        metaconfig_zk_path = METACONFIG_ZK_PATH_FORMAT.format(name)

        try:
            return ZKBaseConfigManager(self.zk_hosts, metaconfig_zk_path,
                                       self.aws_keyfile, self.s3_bucket,
                                       s3_endpoint=self.s3_endpoint).update_zk(None, content, force_update=True)
        except Exception as e:
            raise Exception("Failed to create MetaConfig: %s", e)

    def metaconfig_exists(self, name):
        """
        Check if the metaconfig already exists.

        Args:
            name: the name of metaconfig.

        Returns:
            True if exists, False if not.
        """
        metaconfig_zk_path = METACONFIG_ZK_PATH_FORMAT.format(name)
        if self._kazoo_client().exists(metaconfig_zk_path):
            return True
        else:
            return False

    def create_default_metaconfig_for_config(self, name, config_zk_path):
        """
        A shortcut to create a metaconfig for config using the default template.
        Also create the zk node for the config according to the zk_path passed in.

        Args:
            name: the name of config. For example,
                config.manageddata.admin.decider
            config_zk_path: the zookeeper path mapping to this config

        Returns:
            throw exception if creation failed. Otherwise return the metaconfig body.
        """

        self._kazoo_client().ensure_path(config_zk_path)

        download_command = self.construct_zk_download_data_command_for_config(config_zk_path)
        metaconfig_content = self.construct_metaconfig_with_single_section(
            name, config_zk_path, download_command)
        try:
            self.update_metaconfig(name, metaconfig_content)
        except:
            # Fail to create. Delete the path
            self.remove_metaconfig(name)
            raise
        return metaconfig_content

    def create_default_metaconfig_for_serverset(self, name, serverset_zk_path):
        """
        A shortcut to create a serverset using the default template.
        Also create the zk node for the serverset according to the zk_path passed in.

        Args:
            name: the name of serverset. For example, /discovery/dataservices/prod
            serverset_zk_path: the zookeeper path mapping to this serverset

        Returns:
            throw exception if creation failed. Otherwise return the metaconfig body
        """
        self._kazoo_client().ensure_path(serverset_zk_path)

        download_command = self.construct_zk_download_data_command_for_serverset(serverset_zk_path)
        metaconfig_content = self.construct_metaconfig_with_single_section(
            name, serverset_zk_path, download_command, is_serverset=True)
        self.update_metaconfig(name, metaconfig_content)
        return metaconfig_content

    def update_metaconfig_with_dict(self, name, metaconfig_content_dict):
        """
        Update the metaconfig content with the python dict format.
        Args:
            name: the name of config. For example, discovery.serverset.prod
            content: the content of the MetaConfig, should be in dict format.

        Returns:
            True if successfully updated, otherwise execption is thrown.
        """
        content = simplejson.dumps(metaconfig_content_dict)
        return self.create_metaconfig(name, content)

    def _construct_single_metaconfig_section(self, config_section_name, zk_path,
                                             command, max_wait_in_secs=0,
                                             is_serverset=False):
        """
        Used to construct the json content of a MetaConfig section.
        A MetaConfig can have different sections, which is an array of multiple section dict in JSON format.
        Args:
            config_section_name: this is for backward compatibility. When we use old version of MetaConfig
            as a file on the disk, this is used for python config parser to get metaconfig body. ZUM also
            keeps it.
            zk_path: the zookeeper path of the config (NOTE: not the metaconfig)
            command: the command which tells zk_download_data.py how to download the data to local disk.
            max_wait_in_secs: seconds that ZUM will delay the download of configs once get an update. Usually 0.
            is_serverset: whether this MetaConfig is for serverset? False if it is for ConfigV1 /ConfigV2.

        Returns:
            A dict of single metaconfig section.
        """

        content_dict = {"config_section_name": config_section_name,
                        "zk_path": zk_path,
                        "max_wait_in_secs": max_wait_in_secs,
                        "command": command}

        content_dict['type'] = "serverset" if is_serverset else 'config'

        return content_dict

    def construct_metaconfig_with_single_section(self, config_section_name, zk_path, command,
                                                 max_wait_in_secs=0, is_serverset=False):
        """
        Ideally a metaconfig should have only one section.
        This method is used for creating a metaconfig with only one section.
        Args:
            config_section_name: this is for backward compatibility. When we use old version of MetaConfig
            as a file on the disk, this is used for python config parser to get metaconfig body. ZUM also
            keeps it.
            zk_path: the zookeeper path of the config (NOTE: not the metaconfig)
            command: the command which tells zk_download_data.py how to download the data to local disk.
            max_wait_in_secs: seconds that ZUM will delay the download of configs once get an update. Usually 0.
            is_serverset: whether this MetaConfig is for serverset? False if it is for ConfigV1 /ConfigV2.

        Returns:
            A json blob of metaconfig content.
        """

        metaconfig = [self._construct_single_metaconfig_section(
            config_section_name, zk_path, command, max_wait_in_secs=max_wait_in_secs,
            is_serverset=is_serverset)]
        return simplejson.dumps(metaconfig)

    # some utils
    def construct_zk_download_data_command_for_serverset(self, serverset_zk_path, serverset_file_name=None):
        if not serverset_zk_path.startswith("/discovery/"):
            raise Exception("The serverset zk path should start with "
                            "'/discovery/'. For example: /discovery/userservice/prod")
        if not serverset_file_name:
            serverset_file_name = ".".join(serverset_zk_path.split("/")[1:])
        return ZK_DOWNLOAD_DATA_SERVERSET_FORMAT.format(serverset_file_name, serverset_zk_path)

    def construct_zk_download_data_command_for_config(self, config_zk_path,
                                                      config_local_filename=None,
                                                      s3_path=None):
        if not config_zk_path.startswith("/config/"):
            raise Exception("The configv2 zk path should start with '/config/', "
                            "for example /config/manageddata/growth/popular_nux_topic_list "
                            "or /config/services/pinlater")

        if not config_local_filename:
            config_local_filename = ".".join(config_zk_path.split("/")[1:])

        if not s3_path:
            s3_path = "/data{}".format(config_zk_path)
            return ZK_DOWNLOAD_DATA_CONFIGV2_FORMAT.format(config_local_filename, s3_path, config_zk_path)

    def remove_metaconfig(self, metaconfig_name):
        metaconfig_path = METACONFIG_ZK_PATH_FORMAT.format(metaconfig_name)
        metaconfig_lock_path = metaconfig_path + ".lock"
        transaction = self._kazoo_client().transaction()
        transaction.delete(metaconfig_path)
        transaction.delete(metaconfig_lock_path)
        transaction.commit()

    def get_all_metaconfigs(self):
        parent_node = "/metaconfig/metaconfig"
        children = self._kazoo_client().get_children(parent_node)
        return [child for child in children if not child.endswith(".lock")]

    def get_all_dependencies(self):
        parent_node = "/metaconfig/dependency"
        children = self._kazoo_client().get_children(parent_node)
        return [child for child in children if not child.endswith(".lock")]