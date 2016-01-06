# KingPin

KingPin is the Python toolset used at Pinterest for facilitating service oriented architecture and application configuration management.


## Key Packages & Features
- **Kazoo Utils**: A wrapper for Kazoo and include utils like service registration and local file watcher.
- **Thrift Utils**: A wrapper for python Thrift clients which supports service discovery and connection pool management.
- **Config Utils**: A system that stores configuration on S3, using Zookeeper as the notification system.
- **ZK Update Monitor**: A daemon caching configurations and serversets into local disk from Zookeeper and S3.
- **Decider**: A common utility widely used at Pinterest which controls the global values. Built on ConfigV2. 
- **Managed Datastructure**: A common utility wide used at Pinterest whcih supports easy access/modification of configurations in Map/List format.
- **MetaConfig Manager**: A system that manages all configuration and dependencies, built on top of Zookeeper and S3.


## High level Concepts
Kingpin contains various python packages which serve as the basic infrastructure LEGO widely used at Pinterest. These packages interactive 
with each other to forming a complete solution. 

![Package Architecture](https://cloud.githubusercontent.com/assets/15947888/12130580/6a293310-b3c0-11e5-9e78-bb8c62baf4e4.png)

KingPin relies on 3rd party packages like Kazoo(1.0, the zookeeper client impl in Python), Boto S3 APIs (the S3 client impl in Python), 
Zookeeper and AWS S3. Zookeeper is used to store serversets and serve as the notification hub for changes, S3 is used to store configuration 
data. 

On top of Zookeeper and Kazoo, we have Kazoo Utils which we have some improvements on Kazoo native APIs and does service discovery and registration. 
We use thrift as the RPC protocal, so we built thrift-utils on top of thrift and kazoo_utils which make people easier to write a python thrift client. 

We also have our own application configuration management built on top of Zookeeper and S3 which we call it Config Utils (we call it ConfigV2 internally). 
Basically people can make change to a configuration, the system will put the new content to a separate versioned file in S3, and update the version number in Zookeeper. 
You can refer to this [engineering blog](https://engineering.pinterest.com/blog/serving-configuration-data-scale-high-availability) which contains more information inside. 

We also built some data structures like lists, hashmap and json for Pinterest Engineers to easily read/write to configurations. We also have a structure called Decider (ranging from 0 to 100) 
for engineers to dynamically change values in realtime. 

Both application configuration management and service discovery are watched and updated by a process running on each box called ZK Update Monitor. Our philosophy is to leverage
the atomic broadcasting nature in Zookeeper to fanout the change to every box so each machine only needs to talk to local file, instead of establishing a session to Zookeeper. ZK Update Monitor
is the only proxy talking to Zookeeper on each box for getting the newest serverset of configuration content. We have an [engineering blog](https://engineering.pinterest.com/blog/zookeeper-resilience-pinterest) 
which contains the more details about it.

Another framework we provide in KingPin is called MetaConfig Manager. We found that sometimes we need to manage the dependency like ```what cluster needs what configuration or serverset```. 
MetaConfig is basically a configuraiton which tells ZK Update Monitor how to download a particular configuration or serverset. The MetaConfig Manager stores the dependency graph in Zookeeper and 
metaconfig in S3 (which is built on top of Config Utils). If a box want to add a serverset or configuration dependency, simple call MetaConfig API and the new config will be added to all subscribers 
immediately.

## Configurations
Here are some steps you need to configure before start using KingPin.

### Setting Up AWS and S3 credentials file
In order to talk to S3, you need to prepare a boto (yaml) config file with aws keys in place.
We provide an template for boto S3 config file in ```kingpin/examples/example_aws_keyfile.conf```. 
Replacing the ```aws_access_key_id``` and ```aws_secret_access_key``` with your AWS keypairs under ```Credentials```
section to configure your aws/s3 acecss. Prepare the file in some place ready for future use.

### Install Zookeeper
If you don't have a [Zookeeper](https://zookeeper.apache.org/) cluster and want to try KingPin out on local machine, you can [download it](http://mirror.metrocast.net/apache/zookeeper/zookeeper-3.4.7/).

After downloaded, untar it and run:
```sh
cd zookeeper-3.4.7
cp conf/zoo_sample.cfg conf/zoo.cfg
sudo bin/zkServer.sh start
```

Test whether it works by trying to connect:
```sh
bin/zkCli.sh -server 127.0.0.1:2181
```

In case you want to stop fo Zookeeper instance:
```sh
sudo bin/zkServer.sh stop
```

We use a file to list a set of Zookeeper endpoints. For the local Zookeeper single node case, we provided a 
file called ```local_zk_hosts``` under examples/. You can refer to that file for future use.

### Install Thrift
Follow the instructions [in this page](https://thrift.apache.org/docs/install/) for installing thrift library.

### Install Dependent Packages and KingPin
KingPin is a python package, we suppose you already have Python and [Pip](https://pip.pypa.io/en/stable/) installed. 
In order not to screw up with your exsiting Python libs, we recommend using [Virtual Environments](http://docs.python-guide.org/en/latest/dev/virtualenvs/) 
as the clean container of KingPin and its dependencies. 

#### Install Venv
```sh
cd kingpin
pip install virtualenv
virtualenv env
source venv/bin/activate
```

#### Install KingPin Dependency Packages
```sh
pip install -r requirements.txt
```

#### Install KingPin
```sh
python setup.py install
```

After this, KingPin will be installed in your venv, 
and you can run KingPin scripts directly in virtual environment.

#### Unit Tests
```sh
nosetests
```

## Usage
Here we provide 2 typical usages of KingPin: Application Configuration Management which deploys configurations to every box in real time,
and Service Discovery which uses thrift as the RPC mechanism. We assume you already follow steps in ```Configurations``` and have Zookeeper 
up and running, S3 key file set up and KingPin properly installed.

### Application Configuration Management
Here is an example of creating a manageddata, updating it and let ZK Update Monitor download the content.

The application configuration framework consists of 3 parts: Manageddata or Decider on top of Config Utils, ZK Update Monitor, and MetaConfig Manager. 

![Package Architecture](https://cloud.githubusercontent.com/assets/15947888/12151080/1f5d0698-b462-11e5-8c4f-78809cee3ec3.png)

In the following example, we walk through the process an application subscribes an managed list configuration, and watches the changes and download to local disk. 
Decider works almost the same except the difference in APIs.

#### Creating a ManagedData
The name of manageddata has a Domain and a Key. The domain is like the group, the key is like the member. For example, 
a typical managedlist used inside Pinterest is called "config.manageddata.spam.blacklist". Here "spam" is the domain,
"blacklist" is the key.

Run the following command for starting the an interactive tool for creating a manageddata configuration:
```sh
cd kingpin
python examples/metaconfig_shell.py -z examples/local_zk_hosts -a examples/example_aws_keyfile.conf -b [Your S3 Bucket] -e s3.amazonaws.com
```

Type the command number or name 

#### Creating a Dependency


#### Changing the content of ManagedData configurations



### Thrift and Service Discovery

#### Joining a serverset
When a service host is started, it needs to register itself 

#### Using Thrift Utils to make service calls


## Contact
[ Shu Zhang ](mailto:shu@pinterest.com)
[ @ShuZhang1989 ] (https://github.com/shuzhang1989)


## Contributors (in Alphabetical Order)
Xiaofang Chen, Tracy Chou, Dannie Chu, Pavan Chitumalla, Steve Cohen, Jayme Cox, 
Michael Fu, Jiacheng Hong, Xun Liu, Yash Nelapati, Aren Sandersen, Aleksandar Veselinovic, Chris Walters, Yongsheng Wu, Shu Zhang


## License

Copyright 2015 Pinterest, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. [See the License](LICENSE.txt) for the specific language governing permissions and limitations under the License.