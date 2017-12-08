#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import calendar
import fcntl
import os
import sys
import staticconf
from datetime import datetime

import boto.sqs
import yaml
from pytz import utc
from collections import namedtuple


LOCKS_DIR = 'locks'


def load_package_config(filename, field='module_config', flatten=True, log_keys_only=True):
    """Load the contents of a yaml configuration file using
    :func:`staticconf.loader.YamlConfiguration`, and use
    :func:`configure_packages` to load configuration at ``field``.

    Usage:

    .. code-block:: python

    load_package_config('config.yaml')

    :param filename: file path to a yaml config file with a `meta-config`
                     section
    :param field: field in the file `filename`, which contains a `meta-config`
                  section, which can be read by :func:`configure_packages`.
                  Defaults to `module_config`.
    :param flatten: boolean for whether staticconf should load each module
                  config with a flattened dictionary. Defaults to True.
    :param log_keys_only: boolean for whether staticonf should only log unknown keys
    """
    config = staticconf.YamlConfiguration(
        filename,
        flatten=flatten,
        log_keys_only=log_keys_only,
    )
    configs = config.get(field)
    if configs is None:
        log.warning('Field: {field} was not found in {filename}'.format(
            field=field,
            filename=filename
        ))
    else:
        configure_packages(
            configs,
            flatten=flatten,
            log_keys_only=log_keys_only,
        )
    return config


def configure_packages(configs, flatten=True, log_keys_only=True):
    """Load configuration into a :class:`staticconf.config.ConfigNamespace`
    and optionally call an initialize function after loading configuration
    for a package.


    `configs` should be a `meta-config`, which is a list of configuration
    sections.  Each section **must** have a `namespace` key and *may* have
    one or more other keys:

    namespace
        the name of a :class:`staticconf.config.ConfigNamespace` which will hold
        the configs loaded from a `file` or `config` in this section

    file
        the path to a yaml file. The contents of this file are loaded
        using :func:`staticconf.loader.YamlConfiguration`

    config
        a yaml mapping structure. The contents of this mapping are loaded
        using :func:`staticconf.loader.DictConfiguration`

    For each section in the `meta-config` the following operations are
    performed:

        1. Load the contents of `file`, if the key is present
        2. Load the contents of `config`, if the key is present. If values
           were loaded from `file`, these new `config` values will override
           previous values.

    Example configuration:

    .. code-block:: yaml

        module_config:
            -   namespace: prod_ns
                config:
                    access_log_name: tmp_service_<service_name>
                    error_log_name: tmp_service_error_<service_name>
            -   namespace: memcache
                config:
                    clients:
                        encoders:
                            service_prefix: '<service_name>_service'

    Usage:

    .. code-block:: python

        configs = {...} # The contents of the example configuration above
        configure_packages(configs['module_config'])

    :param configs: List of config dicts.
    :param flatten: boolean for whether staticconf should load each module
        config with a flattened dictionary. Defaults to True.
    :param log_keys_only: boolean for whether staticonf should only log unknown keys
    """
    for config in configs or []:
        # 1st load a yaml
        if 'file' in config:
            staticconf.YamlConfiguration(
                config['file'],
                namespace=config['namespace'],
                flatten=flatten,
                log_keys_only=log_keys_only,
            )

        # 2nd update with a config dict
        if 'config' in config:
            staticconf.DictConfiguration(
                config['config'],
                namespace=config['namespace'],
                flatten=flatten,
                log_keys_only=log_keys_only,
            )


def get_current_time_with_utc_timezone():
    """Returns a datetime object corresponding to now (the current time) with the timezone set to UTC"""
    return datetime.now(utc)


def format_dt_as_iso8601(timestamp):
    """Parses a datetime object and returns it as a string in iso8601 format

    Args:
        timestamp (datetime): a datetime object
    Returns:
        A string representing the timestamp in the iso8601 format
    Raises:
        ValueError when datetime object has no timezone
    """
    utc_timestamp = timestamp.astimezone(utc)
    return utc_timestamp.isoformat()


def datetime_to_unixtime(dt_obj):
    """Converts datetime timestamp into a unix timestamp (as an int)

    Args:
        dt_obj (datetime): a datetime object
    Returns:
        A unix timestamp as an int
    """
    return calendar.timegm(dt_obj.utctimetuple())


def read_content_from_file(filename):
    """Simply reads content from a file.

    Used so we don't have to mock __builtin__.open()

    Args:
        filename (string): name of the file
    """
    with open(filename, 'r') as fh:
        content = fh.read()
    return content


def write_content_to_file(filename, content):
    """Simply writes content to a file.

    Used so we don't have to mock __builtin__.open()

    Args:
        filename (string): name of the file
        content (string): content to write to the file
    """
    with open(filename, 'w') as fh:
        fh.write(content)


def lock_and_load(log_feeder_obj):
    """Sets up a lock so that only one instance of the batch runs at a time, and then loads (starts) the batch

    Args:
        log_feeder_obj (subclass of SQS_Feeder)
    """
    # Only make the log if we are not running in stateless mode
    if not log_feeder_obj.options.stateless:
        # Create locks folder
        if not os.path.isdir(LOCKS_DIR):
            os.mkdir(LOCKS_DIR)
        lock_file_name = '{0}_feeder_batch_{1}.lock'.format(
            log_feeder_obj.APP_NAME, log_feeder_obj.__class__.__name__)
        lock_file_path = os.path.join(LOCKS_DIR, lock_file_name)

        with open(lock_file_path, 'w') as fp:
            file_was_locked = False
            try:
                # If another instance of this batch script is running, this will throw an IOError
                fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
                file_was_locked = True
                # Single instance enforced. Let's run the batch script
                log_feeder_obj.start()
            except IOError as e:
                if e.errno == 11 and not sys.stdout.isatty():
                    # If we can't obtain the lock, exit instead of allowing the exception to surface
                    # allow the exception if we are running interactively though
                    exit(1)
                raise
            finally:
                if file_was_locked:
                    os.remove(lock_file_path)
    else:
        log_feeder_obj.start()


def read_yaml_file(yaml_file, required_keys=[]):
    """Reads entries from a simple, flat YAML file

    Args:
        yaml_file: a YAML file
        required_keys: keys that must be included in the yaml_file
    Returns:
        A dictionary of YAML content
    """
    yaml_content = yaml.load(read_content_from_file(yaml_file))

    for key in required_keys:
        if key not in yaml_content:
            raise KeyError('The file "{0}" does not contain the required key "{1}" in the YAML format'.format(
                yaml_file, key))

    return yaml_content


def get_sqs_queue(aws_config_file):
    """Uses parameters in aws_config_file to authenticate to the SQS service and retrieve an SQS Queue

    Args:
        aws_config_file: a yaml file containing AWS credentials
    Returns:
        An SQS queue object
    """
    conn = get_aws_connection(aws_config_file, boto.sqs.connect_to_region)
    # Open a connection to an SQS instance
    yaml_content = read_yaml_file(aws_config_file, ['queue_name'])
    queue_name = yaml_content['queue_name']

    queue = conn.get_queue(queue_name)
    if queue is None:
        raise NameError('Cannot get the AWS queue "{0}". Are you sure it exists and you have the proper '
                        'permissions to access it?'.format(queue_name))
    return queue


def get_aws_connection(aws_config_file, conn_fetcher):
    """Uses parameters in aws_config_file to open and return an AWS connection object

    Args:
        aws_config_file: a yaml file containing AWS credentials
        conn_fetcher: an AWS method used to fetch the connection object
    Returns:
        An AWS connection object
    """
    yaml_content = read_yaml_file(aws_config_file, ['region_name'])
    region_name = yaml_content.get('region_name', None)

    key_id = yaml_content.get('aws_access_key_id', None)
    secret_key = yaml_content.get('aws_secret_access_key', None)

    # Try to connect to AWS region
    conn = conn_fetcher(region_name, aws_access_key_id=key_id, aws_secret_access_key=secret_key)
    return conn
