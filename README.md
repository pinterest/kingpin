**Note:** This project is no longer actively maintained by Pinterest.

---

# KingPin

[![Build Status](https://travis-ci.org/pinterest/kingpin.svg)](https://travis-ci.org/pinterest/kingpin)

KingPin is the Python toolset used at Pinterest for facilitating service oriented architecture and application configuration management.


## Key Packages & Features
- **Kazoo Utils**: A wrapper for Kazoo and implements utils like ServerSet, DataWatcher and Hosts selectors.
- **Thrift Utils**: A wrapper for python Thrift clients which transparently handles retry and integrated with service discovery framework provided in Kazoo Utils.
- **Config Utils**: A system that stores configuration on S3 and uses Zookeeper as the notification system to broadcast updates to subscribers.
- **ZK Update Monitor**: A daemon which syncs configurations and serversets to local disk from Zookeeper and S3.
- **Decider**: A common utility widely used at Pinterest which controls the global values. Built on top of Config Utils.
- **Managed Datastructure**: A common utility wide used at Pinterest which supports easy access/modification of configurations in Map/List format in Python.
- **MetaConfig Manager**: A system that manages all configurations/serversets and dependencies (subscriptions), built on top of Zookeeper and S3.


## High level Concepts
Kingpin contains various python packages which serve as the basic infrastructure LEGO widely used at Pinterest.  

![Package Architecture](https://cloud.githubusercontent.com/assets/15947888/12130580/6a293310-b3c0-11e5-9e78-bb8c62baf4e4.png)

KingPin relies on 3rd party packages like Kazoo, Boto, Zookeeper and AWS S3. 

Zookeeper is used to store dynamic serversets and acts as the notification channel for config changes. S3 is used to store configuration data. 
On top of Zookeeper and Kazoo, we have Kazoo Utils which we have some improvements on Kazoo native APIs and does service discovery and registration. 
We use thrift as the RPC protocal, so we built thrift-utils on top of thrift and kazoo_utils which make people easier to write a python thrift client. 

We also have our own application configuration management system built on top of Zookeeper and S3 which we call it Config Utils (we call it ConfigV2 internally). 
Basically people can make change to a configuration, the system will put the new content to a new versioned file in S3, and update the version number in Zookeeper. 
You can refer to this [engineering blog](https://engineering.pinterest.com/blog/serving-configuration-data-scale-high-availability) which contains more information inside. 

We also built some data structures like lists, hashmap and json for Pinterest Engineers to easily read/write to configurations. 
We also have a structure called Decider (ranging from 0 to 100) for engineers to dynamically change values in realtime, which is pretty useful for experiments control. 

Both application configuration management and service discovery are watched and updated by a process running on each box called ZK Update Monitor. Our philosophy is to leverage
the atomic broadcasting guarantee provided by Zookeeper to fanout the change to every box so each machine only needs to talk to local file, instead of establishing a session to Zookeeper. ZK Update Monitor
is the only proxy talking to Zookeeper on each box for getting the newest serverset of configuration content. 
We have an [engineering blog](https://engineering.pinterest.com/blog/zookeeper-resilience-pinterest) which contains the more details about it.

Another framework we provide in KingPin is called MetaConfig Manager. We found that sometimes we need to manage the dependency like ```what cluster needs what configuration or serverset```. 
MetaConfig is basically a configuraiton which tells ZK Update Monitor how to download a particular configuration or serverset. The MetaConfig Manager stores the dependency graph in Zookeeper and 
metaconfigs in S3. If a box want to add a serverset or configuration dependency, simply call MetaConfig API and the new config will be added to all subscribers 
immediately.

## Configuration and Installation
Here the steps how to make KingPin running.

### Python 2.7.3
KingPin is tested under Python 2.7.3 at Pinterest scale. We highly recommend you run KingPin using Python 2.7.3.

### Install Pip
Please follow [this website](https://pip.pypa.io/en/stable/installing/) to make sure pip is installed on your box:
```sh
wget https://bootstrap.pypa.io/get-pip.py
python get-pip.py
```

### Setting Up AWS and S3 credentials file
In order to talk to S3, you need to prepare a boto (yaml) config file with aws keys in place.
We provide an template for boto S3 config file in ```kingpin/examples/example_aws_keyfile.conf```. 
Replacing the ```aws_access_key_id``` and ```aws_secret_access_key``` with your AWS keypairs under ```Credentials```
section to configure your aws/s3 acecss. Prepare the file in some place ready for future use.

### Install Zookeeper
If you don't have a [Zookeeper](https://zookeeper.apache.org/) cluster and want to try KingPin out on local machine, you can [download it](http://apache.mirrors.lucidnetworks.net/zookeeper/zookeeper-3.4.6/).

After downloaded, untar it and run:
```sh
cd zookeeper-3.4.6
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
file ```examples/local_zk_hosts```. There should only be one liner there which points to the localhost zookeeper node.
You need to refer to that file for future use.


### Install Thrift
Follow the instructions [in this page](https://thrift.apache.org/docs/install/) for installing thrift library.

### Install KingPin
KingPin is a python package, we suppose you already have Python and [Pip](https://pip.pypa.io/en/stable/) installed.

In order not to screw up with your exsiting Python libs, we recommend using [Virtual Environments](http://docs.python-guide.org/en/latest/dev/virtualenvs/) 
as the clean container of KingPin and its dependencies. 

#### Install Venv
```sh
cd kingpin
pip install virtualenv
virtualenv env
virtualenv -p /usr/bin/python2.7 venv
source venv/bin/activate
```

#### Install KingPin Dependency Packages
```sh
cd kingpin
pip install -r requirements.txt
```
If you get some error when installing gevent, you may need to install [libevent](http://libevent.org/) first.'
If after installing libevent you still get the header file cannot found error, you can manually install gevent by running:
```sh
CFLAGS="-I /usr/local/include -L /usr/local/lib" pip install gevent==0.13.8
```
Note that the INCLUDE and LIB path may vary, both are the actual places you libevent is installed.

After you get this error fixed, you may need to run ```pip install -r requirements.txt```
again to finish the dependency installation.

#### Install KingPin
```sh
cd kingpin
sudo python setup.py install
```

### Install Supervisor
ZK Update Monitor is running inside [Supervisor](http://supervisord.org/). Supervisor makes ZK Update Monitor to run under the supervisord container. 

We also recommend installing supervisor under Venv. To install supervisor, simply run:
```sh
pip install supervisor
```

We provided an example supervisor configuration under examples/ directory. The configuration has some fields to fill in like dependency name and S3 bucket name. 

Once you filled in the blanks in the configuration, you can run ZK Update Monitor via Supervisor under KingPin Directory:

```sh
cd kingpin
sudo supervisord -c examples/supervisor_zk_update_monitor.conf
```
You can verify if zk_update_monitor is running by:
```sh
ps aux | grep zk_update_monitor
```

### Unit Tests
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

![Architecture](https://cloud.githubusercontent.com/assets/15947888/12151080/1f5d0698-b462-11e5-8c4f-78809cee3ec3.png)

In the following example, we walk through the process that an application subscribes a managed list, and watches the changes and download to local disk. 
Decider works almost the same except the difference in APIs. We provide a script in ```examples/metaconfig_shell.py``` which help you easily 
create conifg and dependencies. 

Of course, on top of the config APIs, you can easily build a fancier UI.

#### Creating Local Directory for Storing Configs
Before doing everything, you need to create a directory under /var/ path using sudo permission:
```sh
sudo mkdir /var/config
```

#### Creating a ManagedData
The name of manageddata has a ```Domain``` and a ```Key```. The domain is like the group, the key is like the member. For example, 
a typical managedlist used inside Pinterest is called ```config.manageddata.spam.blacklist```. Here "spam" is the domain,
"blacklist" is the key.

Run the following command for starting the an interactive tool for creating a manageddata configuration:
```sh
cd kingpin
python examples/metaconfig_shell.py -z examples/local_zk_hosts -a examples/example_aws_keyfile.conf -b [Your S3 Bucket to Put Config Data] -e s3.amazonaws.com
```

Type "2" -> "1" to create a manageddata, and give the domain and key name. Let's give the domain called "test", key called "test_config". 
The created manageddata is then called ```config.manageddata.test.test_config```. Remember this name for future use.

#### Creating a Dependency
The manageddata should be created. Now we need to create a dependency. 
A dependency is a collection of manageddata or serverset which tells the subscription of a set of serversets or configurations. 
ZK Update Monitor need to know the dependency of the localbox so it can subscribe and downlaod corresponding configurations.

Run the ```metaconfig_shell.py``` again, and type "1" and create a dependency. In order to tell the difference between a dependency and a configuration internally, we require dependency names to be ended with ```.dep```. 

Let's create a a dependency called ```test_dependency.dep```.

#### Adding the Manageddata to a Dependency
Then we should be able to add the created manageddata to the created dependency. Run ```metaconfig_shell.py``` and type "3", type ```test_dependency.dep``` and ```config.manageddata.test.test_config``` respectively to add the manageddata config into the dependency.

#### Running ZK Update Monitor
Don't forget to start ZK Update Monitor. You need to replace the ```[dependency-name]``` in the Zk Update Monitor command line inside supervisord configuration (which is localed in ```examples/supervisor_zk_update_monitor.conf```
 to "test_dependency.dep", also don't want to change the bucket name.

Now ZK Update Monitor is watching changes of the configuration called "config.manageddata.test.test_config". You can double check by querying the Zk Update Monitor admin flask endpoint:

```sh
curl 127.0.0.1:9898/admin/watched_metaconfigs
```

It should show the watched configuration list, currenly should be only "config.manageddata.test.test_config".

#### Changing the content of ManagedData configurations
Now we can try change the content of the manageddata configuration. For now the modification can be done in the python shell. Inside Pinterest we use a web portal for people the change the content.
Suppose the configuration will be a managedlist.

Bring up python.
```sh
python
```

Try add a value into the managedlist.
```python
from kingpin.manageddata.managed_datastructures import ManagedList
managedlist = ManagedList("test", "test_config", "test config", "a config for test", ["127.0.0.1:2181"], "examples/example_aws_keyfile.conf", "some_test_bucket")
managedlist.add("test_data")
```

If no error happens, you can check the content of the managedlist which is ```/var/config/config.manageddata.test.test_config```. 
There should be one item called ```test_data``` inside. 

### Thrift and Service Discovery
In this example we bring up a test thrift server locally, register it to a serverset and use thrift_util 
to talk to the test server.

![Architecture](https://cloud.githubusercontent.com/assets/15947888/12178908/f293f14a-b528-11e5-8ed9-a1fb3a1541ef.png)

#### Creating Local Directory for Storing Serversets
Before doing everything, you need to create a directory under /var/ path using sudo permission:
```sh
sudo mkdir /var/serverset
```

#### Creating a serverset
Let's create the serverset of the service "test_service". Running in a environment called "prod". The format of a serverset name is discovery.{service name}.{service environment}.
So the serverset name in this case will be ```discovery.test_service.prod```. 

Run the metaconfig_shell again to create the serverset:

```sh
cd kingpin
python examples/metaconfig_shell.py -z examples/local_zk_hosts -a examples/example_aws_keyfile.conf -b [Your S3 Bucket to Put Config Data] -e s3.amazonaws.com
```
Type "2" -> "2" to create a serverset, and give the service name and service environment. Let's give the service name called "test_service", environment called "prod". 

#### Adding the serverset to dependency
Suppose you have already created the dependency called "test_dependency.dep". 
We can add the serverset to the dependency so ZK Update Monitor can watch any change of the serverset:

```sh
cd kingpin
python examples/metaconfig_shell.py -z examples/local_zk_hosts -a examples/example_aws_keyfile.conf -b [Your S3 Bucket to Put Config Data] -e s3.amazonaws.com
```
Type "3" to add the serverset ```discovery.test_service.prod``` to ```test_dependency.dep``` as the instruction shows.


#### Joining a serverset
When a service host is started, it needs to register itself to the serverset so client can know the endpoint to connect to.
We delegate the registration to another daemon called ```zk_register```. 

Try starting zk_regsiter to register the local host to discovery.test_service.prod as port 8081:

```sh
cd kingpin
zk_register.py -p 8081 -s test_service -e prod -z examples/local_zk_hosts
```

The command will hang because it need to keep a session to Zookeeper to keep an ephemeral node up representing this host. To verify the updated
serverset is observed by ZK Update Monitor, go to ```/var/serverset/```, there should be a file called ```discovery.test_service.prod``` and it 
should have one line which is the endpoint of localhost the service will be running on.


##### Notice
One thing you may need to put special care is the failure case - if the service process is dead but the zk_register is still running,
clients may talk to an endpoint with no service running on that port. Here are suggestions to prevent that:
1. Have some extra logic checking the healthness of the server inside zk_register so it will stop registering if the healthcheck fails.
2. Put the registration part inside the service process. You may need to implement the registration logic in other languages. 

#### Using Thrift Utils to make service calls
We provide an example thrift definition in examples/ directory called ```test_service.thrift```.

Run the following command to get the generated code in Python:
```sh
cd kingpin
thrift -r --gen py examples/test_service.thrift
```

#### Start the Python Thrift Server
We provided an server startup script under examples/test_service_server.py. After the 
python thrift code is generated, move the ```test_service_server.py``` under gen-py/test_service.

Run ```test_service_server``` and the server will be running on port 8081.

#### Make a call to thrift server using Thrift_utils
Here are an example using Thrift Util to construct a wrapper thrift client which has features like 
connection pool management, dynamic serverset management and retry management, etc:

```python
from kingpin.thrift_utils.thrift_client_mixin import PooledThriftClientMixin
from kingpin.thrift_utils.base_thrift_exceptions import ThriftConnectionError
from kingpin.kazoo_utils.hosts import HostsProvider

import TestService

class TestServiceConnectionException(ThriftConnectionError):
    pass

class TestServiceClient(TestService.Client, PooledThriftClientMixin):
    def get_connection_exception_class(self):
        return TestServiceConnectionException

testservice_client = TestServiceClient(
    HostsProvider([], file_path="/var/serverset/discovery.test_service.prod"),
    timeout=3000,
    pool_size=10,
    always_retry_on_new_host=True)
```

The above example is how we constrcut the thrift client class with Thrift Util features turned on. The class should inherit from your thrift service client (generated by thrift)
annd PooledThriftClientMixed (make sure the generated thrift client class is the first base class). 

You must implement a method called ```get_connection_exception_class``` which returns a class which will be thrown when all retries are failed.

Then you need to initialize an client object. In the constructor, you need to pass in a HostsProvider as the first argument. Passing in the serverset file 
path there so the client can always read the endpoint in the file (which may change dynamically). The frist parameter of HostsProvider constructor is a list
of static endpoint list, we don't recommend using that because it does't provide the benefit of dynamic serverset. 

We also provide an example under ```examples/test_service_client.py```, move it under gen-py/test_service
and run the script, it should output "pong".


## Contact
[ Shu Zhang ](mailto:shu@pinterest.com)
[ @ShuZhang1989 ] (https://github.com/shuzhang1989)


## Contributors (in Alphabetical Order)
Xiaofang Chen, Tracy Chou, Dannie Chu, Lida Li, Pavan Chitumalla, Steve Cohen, Jayme Cox, 
Michael Fu, Jiacheng Hong, Xun Liu, Yash Nelapati, Aren Sandersen, Aleksandar Veselinovic, Chris Walters, Yongsheng Wu, Shu Zhang, Suli Xu


## License

Copyright 2016 Pinterest, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. [See the License](LICENSE.txt) for the specific language governing permissions and limitations under the License.