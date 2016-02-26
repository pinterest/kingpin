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

""" Some utility functions we use for python logging, built on
top of Python native logging package.
It provides formatted layout in the logging file.
In KingPin, this is used by zk_update_montior and zk_download_data processes.


Usage:
from kingpin.logging_utils import initialize_logger
initialize_logger(logger_name='', logger_filename='',
                  log_to_stderr=False, log_dir="/var/log")

"""

import logging
import logging.handlers
import os
import sys

__INITIALIZED = {}

#: Log only `STDERR_LEVEL` and greater.
#: Integer (based on constants in :py:mod:`logging <python:logging>`) to
#: indicate minimum severity to output::
#:
#:  >>> [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR,
#:       logging.FATAL]
#:  [10, 20, 30, 40, 50]
#:
#: Note that we default to :py:const:`logging.INFO`.
#: Can be overridden with environment variable ``PINLOG_STDERR_LEVEL`` by
#: setting it to `DEBUG`, `INFO`, `WARNING`, `ERROR` or `FATAL`.
STDERR_LEVEL = getattr(logging, os.environ.get("PINLOG_STDERR_LEVEL", "INFO"))

#: Nothing below this loglevel is logged (in ``stderr``, a file, ``syslog``,
#: # etc).  This uses the same format as :py:const:`STDERR_LEVEL`.
#: Can be overridden with environment variable ``PINLOG_MIN_LOG_LEVEL``.
MIN_LOG_LEVEL = getattr(logging, os.environ.get("PINLOG_MIN_LOG_LEVEL", "INFO"))

#: Logfiles are written into this directory.  If not set log files are not
#: written to disk.
#: Can be overridden with environment variable ``PINLOG_LOG_DIR``.
LOG_DIR = os.getenv("PINLOG_LOG_DIR")

#: Log files are written to this filename.
#: Can be overridden with environment variable ``PINLOG_LOG_FILE``.
LOG_FILE = os.getenv("PINLOG_LOG_FILE", "/var/log")


def __generate_filename():
    """Generate a log filename based on pid and program name.

    Conventional log filename has the following format:
    ``/<log dir>/<program  name>. <pid>.log``
    (e.g. "/tmp/hello_world.1234.log").

    Returns:
        A string for the log filename.
    """
    program_name = sys.argv[0].split("/")[-1]
    if not program_name:
        program_name = "UNKNOWN-PROGRAM-NAME"
    pid = os.getpid()
    FORMAT = "{program_name}.{pid}.log"
    # Introducing a dictionary in the string interpolation below as PEP8 cannot
    # see variables are used if we picked them up from locals() call.
    return FORMAT.format(program_name=program_name, pid=pid)


def __get_file_formatter():
    """Get logging formatter with Google logging like format.

    Each line in the log should look like:
    [DIWEF]mmdd hh:mm:ss.uuuuuu threadid file:line] <message>

    Returns:
        Formatter object for use in logging handlers.
    """
    # [IWEF]mmdd hh:mm:ss.uuuuuu threadid file:line] <message>
    ASCII_TIME_FORMAT = "%m%d %H:%M:%S"         # mmdd hh:mm:ss.uuuuuu
    LINE_FORMAT = ("%(levelname).1s"            # [DIWEF]
                   "%(asctime)s.%(msecs)s "     # ASCII_TIME_FORMAT
                   "%(threadName)s "            # threadid
                   "%(pathname)s:%(lineno)d] "  # file:line]
                   "%(message)s")               # <message>
    return logging.Formatter(fmt=LINE_FORMAT, datefmt=ASCII_TIME_FORMAT)


def initialize_logger(logger_name='', logger_filename=LOG_FILE, log_to_stderr=False, log_dir=LOG_DIR):
    """Initialize global log.

    Before using the log object, run ``initialize_logger()`` to set up
    handlers and formatting. You should almost never run this
    directly: it is up to ``apprunner.runapp()`` to set up the log
    calling this function.

    Args:
        ``logger_name``: A string specifying the name to use while
            outputting log lines.
        ``logger_filename``: The filename to use within ``LOG_DIR``. If
            not set will use the format indicated by
            :meth:`__generate_filename`.
        ``log_to_stderr``: If true we log to stderr
        ``log_dir``: the directory logs go to.

    Returns the logger object.
    """

    global __INITIALIZED
    logger = logging.getLogger(logger_name)
    if __INITIALIZED.get(logger_name, False):
        return logger

    # Use custom formatter that logs at microseconds level and follows glog
    # format.

    # Translate logging levels.
    stderr_level = STDERR_LEVEL
    logging_level = MIN_LOG_LEVEL

    if log_to_stderr:
        # Setup console handler.
        console_handler = logging.StreamHandler()
        line_format = ""
        line_format += ("%(levelname).1s"  # [DIWEF]
                        "%(asctime)s "     # "%H:%M:%S"
                        "[%(name)s] ")     # [<name>]
        line_format += "%(message)s"       # <message>
        console_handler.setFormatter(logging.Formatter(line_format, datefmt="%H:%M:%S"))
        console_handler.setLevel(max(logging_level, stderr_level))
        logger.addHandler(console_handler)

    if log_dir:
        # Timed rotating file handler has hourly rotation enabled by default.
        if logger_filename:
            filename = os.path.join(log_dir, logger_filename)
        else:
            filename = os.path.join(log_dir,  __generate_filename())
        rotating_file_handler = logging.handlers.TimedRotatingFileHandler(filename, utc=True, when='h')
        rotating_file_handler.setFormatter(__get_file_formatter())
        logger.addHandler(rotating_file_handler)

    logger.setLevel(logging_level)

    # We don't want to propagate log messages. Until we really start having
    # loggers that are initialized with different handlers.
    logger.propagate = False
    __INITIALIZED[logger_name] = True
    return logger