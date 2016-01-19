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

"""An interactive shell written in Python to manage metaconfigs"""

import argparse
from kingpin.metaconfig.metaconfig_utils import MetaConfigManager

# MetaConfigManager
metaconfig_manager = None


def create_dependency():
    print "Please enter the name of the dependency you want to create"
    print '(The dependency name should end with ".dep"'
    dependency_name = raw_input().lower()
    metaconfig_manager.create_dependency(dependency_name)
    print "Dependency {} has been created!".format(dependency_name)
    return


def create_metaconfig():
    print "Please specify the type: config or serverset."
    print "1. ManagedData"
    print "2. Serverset"
    type_index = raw_input("Please specify the type number: ")
    assert type_index in ['1', '2']
    if type_index == "1":
        # create the metaconfig for config
        print "What's the domain name of the manageddata?"
        domain_name = raw_input().lower()
        print "Waht's the key name of the manageddata?"
        key_name = raw_input().lower()
        metaconfig_name = "config.manageddata.{}.{}".format(domain_name, key_name)
        if metaconfig_manager.metaconfig_exists(metaconfig_name):
            print "Metaconfig already created!"
            return
        metaconfig_zk_path = "/config/manageddata/{}/{}".format(domain_name, key_name)
        metaconfig_manager.create_default_metaconfig_for_config(metaconfig_name, metaconfig_zk_path)
        print "Metaconfig {} created!".format(metaconfig_name)
        return
    elif type_index == "2":
        # create the meatconfig for serverset
        print "What's the name of the service whose serverset will be created?"
        service_name = raw_input().lower()
        print "What's the environment of the serverset? For example, prod or integ"
        service_environment = raw_input().lower()
        serverset_zk_path = "/discovery/{}/{}".format(service_name, service_environment)
        metaconfig_name = "discovery.{}.{}".format(service_name, service_environment)
        metaconfig_manager.create_default_metaconfig_for_serverset(
            metaconfig_name, serverset_zk_path
        )
        print "Metaconfig {} created!".format(metaconfig_name)
        return


def view_dependencys():
    print "All dependencies created:"
    all_dependencies = metaconfig_manager.get_all_dependencies()
    for dependency in all_dependencies:
        print dependency
    print "What dependency do you want to view?"
    dependency_name = raw_input().lower()
    assert dependency_name.endswith(".dep")
    members = metaconfig_manager.get_dependency_members(dependency_name)
    print "Members in dependency {}:".format(dependency_name)
    for member in members:
        print member
    return


def add_to_dependency():
    print "What's the dependency name do you want to add to? (name should end with .dep)"
    
    dependency_name = raw_input().lower()
    print "What's the name of Metaconfig or Dependency you want to add?"
    new_member_name = raw_input().lower()
    metaconfig_manager.add_to_dependency(dependency_name, new_member_name)
    print "{} has been successfully added to dependency {}".format(new_member_name, dependency_name)


def view_metaconfigs():
    print "Here is the list of all metaconfigs"
    for metaconfig in metaconfig_manager.get_all_metaconfigs():
        print metaconfig
    print "[DONE]"


def exit_shell():
    exit(0)

instructions_set = [
    ("1. Create a new dependency", create_dependency),
    ("2. Create a new manageddata or a serverset", create_metaconfig),
    ("3. Add a manageddata/serverset or a dependency to dependency", add_to_dependency),
    ("4. View Dependency and its members", view_dependencys),
    ("5. View all metaconfigs", view_metaconfigs),
    ("6. Exit the shell", exit_shell)
]


def main():
    parser = argparse.ArgumentParser(description="MetaConfig Management Shell")
    parser.add_argument(
        "-z", "--zk-hosts-file-path", dest="zk_hosts_file", metavar="ZKHOSTS",
        required=True,
        help="The path of file which have a list of Zookeeper endpoints "
             "(host:port) which keeps the metaconfig as well as "
             "the config/serversets"
    )
    parser.add_argument(
        "-a", "--aws-keyfile", dest="aws_keyfile", metavar="AWSKEY",
        required=True,
        help="The path of the file storing AWS access and secret keys",
    )
    parser.add_argument(
        "-b", "--s3-bucket", dest="s3_bucket", metavar="BUCKET",
        required=True,
        help="The S3 bucket storing metaconfigs / configs"
    )
    parser.add_argument(
        "-e", "--aws-s3-endpoint", dest="s3_endpoint", metavar="ENDPOINT",
        default="s3.amazonaws.com",
        help="The S3 endpoint storing metaconfig / configs"
    )
    args = parser.parse_args()

    zk_hosts = []
    with open(args.zk_hosts_file, "r") as zk_hosts_file:
        for line in zk_hosts_file:
            if line:
                zk_hosts.append(line.strip())

    global metaconfig_manager
    metaconfig_manager = MetaConfigManager(zk_hosts, args.aws_keyfile, args.s3_bucket, s3_endpoint=args.s3_endpoint)

    print "What do you want to do with Metaconfigs / Dependencies?"
    for instruction in instructions_set:
        print instruction[0]

    user_selection = int(raw_input("Enter action number: "))
    assert user_selection in range(1, len(instructions_set) + 1)
    instructions_set[user_selection - 1][1]()


if __name__ == "__main__":
    main()