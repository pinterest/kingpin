#!/usr/bin/python

"""
This is a script that registers services with a serverset in ZK for discovery

Usage:

    zk_register --service <name> --port <port#> --weight <number> --usepublicip --useservicecheck --usehostname
"""


import argparse
import logging
import signal
import sys

from gevent.event import Event
import gevent

from kingpin.kazoo_utils import ServerSet
from kingpin.zk_update_monitor import zk_util

logger = logging.getLogger()


def sigusr1_handler():
    print 'Received SIGUSER1 -- Graceful exit'
    sys.exit(0)

# Set the signal handler
gevent.signal(signal.SIGUSR1, sigusr1_handler)


def validate_port(port):
    """ are the given ports valid TCP/UDP ports? """
    start, end = 1, 65535
    if port < start or port > end:
        raise Exception('Bad port number: %d' % port)


def main():
    global args
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-p", "--port", dest="port", metavar="PORT",
        required=True,
        help="The port of the service runnning on"
    )
    parser.add_argument(
        "-s", "--service-name", dest="service_name", metavar="SERVICE_NAME",
        required=True,
        help="The name of the service you want to register to"
    )
    parser.add_argument(
        "-e", "--environment", dest="service_environment", metavar="SERVICE_ENVIRONMENT",
        required=True,
        help='The environment service is running on, for example prod, staging'
    )
    parser.add_argument(
        "-z", "--zk-hosts-file-path", dest="zk_hosts_file", metavar="ZKHOSTS",
        required=True,
        help="The path of file which have a list of Zookeeper endpoints "
             "(host:port) which keeps the metaconfig as well as "
             "the config/serversets"
    )
    args = parser.parse_args()

    service_name = args.service_name
    service_environment = args.service_environment
    service_port = int(args.port)

    serverset_full_path = '/discovery/%s/%s' % (service_name, service_environment)

    zk_hosts = zk_util.parse_zk_hosts_file(args.zk_hosts_file)

    validate_port(service_port)

    ServerSet(serverset_full_path, zk_hosts).join(service_port, use_ip=True,
                                                  keep_retrying=True)
    Event().wait()


if __name__ == '__main__':
    main()
