#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BaseFeeder is a superclass intended to be subclassed.
"""
from __future__ import absolute_import

import optparse
import os
import sys
import time
import traceback
from datetime import timedelta

import dateutil.parser
import simplejson
import staticconf
import logging
from optparse import OptionParser

from log_feeder.json_logging import JsonLogger
from log_feeder.profiling import measure_elapsed_time
from log_feeder.rate_limiter import RateLimiter
from log_feeder.utils import get_current_time_with_utc_timezone
from log_feeder.utils import read_content_from_file
from log_feeder.utils import write_content_to_file
from log_feeder.utils import load_package_config

LOG_DIR = 'log_files'

def parse_base_feeder_options():
        parser = OptionParser(usage="usage: %prog [options] filename", version="%prog 1.0")
        parser.add_option(
            '-c', '--config_path', default='configs/yelp_logfeeder_config.yaml',
            help='Path to the configuration file. (default=%default)')
        parser.add_option(
            '-i', '--instance_name', default='', help='Name of LogFeeder instance, which will only be used for '
            'creating a lock file for the LogFeeder process. We recommend normally using the account/domain name for '
            'this instance name (default=empty string)')
        parser.add_option(
            '-s', '--start_time', default=None,
            help='Gets events from the API that occurred at or after this "start_time". Specify time in the '
                 'iso8601 format (YY-mm-ddTHH:MM:SSZ). Remember to use UTC, not local time. Defaults to 10 minutes '
                 'before the current time.')
        parser.add_option(
            '-e', '--end_time', default=None,
            help='Gets events from the API that occurred at or before this "end_time". Specify time in the '
                 'iso8601 format (YY-mm-ddTHH:MM:SSZ). Remember to use UTC, not local time. Defaults to the current '
                 'time.')
        parser.add_option(
            '-S', '--relative_start_time', metavar='REL_START', type=int, default=None,
            help='Gets events from the API that occurred at or after REL_START minutes before the '
                 'current time')
        parser.add_option(
            '-E', '--relative_end_time', metavar='REL_END', type=int, default=None,
            help='Gets events from the API that occurred at or before REL_END minutes before the '
                 'current time')
        parser.add_option(
            '-H', '--hidden_tag', metavar='TAG', default='',
            help='[OPTIONAL] Adds the substring TAG to the end of the name of the file storing the last timestamp '
                 'for a run of LogFeeder. This tag will Not appear in the processed API records, and so '
                 'allows multiple instances of LogFeeder running on the same API data to produce identical output '
                 'records, which can then be de-duplicated by logstash, without overwriting each other\'s last '
                 'timestamp files.')
        parser.add_option(
            '--no-output',
            action='store_true',
            help='Runs LogFeeder without feeding anything to the output, instead use stdout')
        parser.add_option(
            '--stateless',
            action='store_true',
            help='Runs LogFeeder without feeding writing the last query time to a file and without using a file lock')
        return parser

class BaseFeeder(object):

    """Reads Activity data from an API and feeds it some output module.
    This requires a Feeder and an Output subclass to function."""

    USERNAME_FIELD = None

    def __init__(self, **kwargs):
        super(BaseFeeder, self).__init__()
        self.log_folder = LOG_DIR
        self.do_write_latest_event_time = True
        self.num_records_processed = 0
        self.latest_record_timestamp = ''
        self.options = kwargs['options']
        self.log = logging.getLogger(self.options.instance_name)


    def get_name(self):
        (filename, ext) = os.path.splitext(os.path.basename(sys.argv[0]))
        return filename

    def load_service_config(self):
        load_package_config(self.options.config_path)

    def parse_config(self, config_file_path):
        """Parses the configuration file

        Args:
            config_file_path (string): path to the configuration file
        """
        # Read main logfeeder configuration file
        staticconf.YamlConfiguration(config_file_path)
        self.aws_config_filepath = staticconf.read('logfeeder.aws_config_filepath')
        self.domain = staticconf.read('logfeeder.domain')
        app_file = staticconf.read('{0}.file'.format(self.APP_NAME))

        # Read app specific configuration file
        contents = staticconf.YamlConfiguration(app_file)
        self.api_creds_filepath = staticconf.read('api_creds_filepath')
        if 'rate_limiter_num_calls_per_timeunit' in contents:
            self.rate_limiter = RateLimiter(
                calls_per_timeunit=staticconf.read_int('rate_limiter_num_calls_per_timeunit'),
                seconds_per_timeunit=staticconf.read_int('rate_limiter_num_seconds_per_timeunit'),
            )
        self.sub_apis = {}
        for key in contents:
            if key.startswith('enable_'):
                name_of_subapi = key.split('enable_', 1)[1]
                self.sub_apis[name_of_subapi] = staticconf.read_bool(key)
        # If an API doesn't have any sub_apis, then set set its APP_NAME to self.sub_apis for code compatibility
        if not self.sub_apis:
            self.sub_apis = {self.APP_NAME: True}

    def read_api_creds(self, api_creds_filepath):
        """(Abstract method) Reads API credentials from a file

        Reads a file containing important info about the API necessary for authentication,
        such as an API key.

        Returns:
            A dictionary containing API credentials
        """
        raise NotImplementedError

    def _make_queries_wrapper(self, *args):
        """ providers an interface for feeders to either return a single
        record generator or a list of record generators. By default, it
        will return the result of _make_queries as a single item list. """
        return [self._make_queries(*args)]

    def get_last_timestamp_filename(self, sub_api_name):
        """Returns the name of the file storing the last timestamp for a run of LogFeeder

        Args:
            sub_api_name (string): name of the sub_api
        Returns:
            The name of the file storing the last timestamp for a run of LogFeeder
        """
        return os.path.join(self.log_folder, '{0}_{1}_{2}_last_timestamp{3}.txt'.format(
            self.APP_NAME, sub_api_name, self.domain, self.options.hidden_tag))

    def _read_last_time(self, sub_api_name):
        """Reads the last time from the prior run of LogFeeder for the given sub_api

        Args:
            sub_api_name (string): name of the sub_api
        Returns:
            A datetime object (with a UTC timezone), or None if the file with the last time does not exist
        """

        # Try to read the file containing the last timestamp
        filename = self.get_last_timestamp_filename(sub_api_name)
        if os.path.exists(filename):
            time_string = read_content_from_file(filename)
        else:
            return None

        datetime_obj = dateutil.parser.parse(time_string)
        return datetime_obj

    def mark_as_completed(self, latest_ts, sub_api_name):
        """ Takes the latest_ts, as iso8601, and writes it to the timestamp file. """
        timestamp_filename = self.get_last_timestamp_filename(sub_api_name)
        write_content_to_file(timestamp_filename, latest_ts)

    def _make_queries(self, start_times_dict, end_time, api_info_dict, sub_api_name):
        """(Abstract method) Makes queries to the API for records in the specified time range

        This method is a generator which yields the API records.

        Args:
            start_time (string): starting time (as a string recognized by the API) for log ingestion
            end_time (string): ending time (as a string recognized by the API) for log ingestion
            api_info_dict: a dictionary containing important API info
            sub_api_name (string): the name of the sub_api to be called (if the API has sub_apis)
        """
        raise NotImplementedError

    def convert_dt_to_api_timestring(self, datetime_obj):
        """(Abstract method) Converts a datetime object to a time string accepted by the API

        Args:
            datetime_obj (datetime): A datetime object
        Returns:
            A string representing the datetime that is accepted by the API
        """
        raise NotImplementedError

    def convert_api_timestring_to_iso8601(self, api_timestring):
        """(Abstract method) Converts a time string output by the API to a iso8601 time string

        Args:
            api_timestring (string): A time string output by the API
        Returns:
            A time string in the iso8601 format
        """
        raise NotImplementedError

    def rename_timestamp_key(self, rec):
        """Changes the default key-name of the primary timestamp associated with an API record to "event_time"

        Args:
            record (dict): a record from an API
        """
        if self.TIMESTAMP_KEYNAME in rec[self.record_label]:
            timestamp_name = self.TIMESTAMP_KEYNAME
        else:
            timestamp_name = self.SECONDARY_TIMESTAMP_KEYNAME
        rec['event_time'] = rec[self.record_label][timestamp_name]
        del rec[self.record_label][timestamp_name]

    def set_time_range(self, sub_api_name):
        """Sets the starting and ending times for the API query

        Args:
            sub_api_name (string): name of the sub_api
        """
        now_ts = get_current_time_with_utc_timezone()

        # First look for absolute timestamps specified via the command line
        if self.options.start_time:
            # Check that absolute and relative time are not simultaneously used
            if self.options.relative_start_time:
                raise ValueError('Both the command-line options "start_time" and "relative_start_time" are used. '
                                 'Either one of them or neither of them should be used.')
            start = dateutil.parser.parse(self.options.start_time)
        else:
            # Next look for relative timestamps specified via the command line
            if self.options.relative_start_time:
                start = now_ts - timedelta(minutes=self.options.relative_start_time)
            else:
                # Last, try reading the starting timestamp from a file
                start = self._read_last_time(sub_api_name)
                if not start:
                    # If the file doesn't exist use a default offset of 10 minutes
                    start = now_ts - timedelta(minutes=10)
        if self.options.end_time:
            if self.options.relative_end_time:
                raise ValueError('Both the command-line options "end_time" and "relative_end_time" are used. '
                                 'Either one of them or neither of them should be used.')
            end = dateutil.parser.parse(self.options.end_time)
        else:
            if self.options.relative_end_time:
                end = now_ts - timedelta(minutes=self.options.relative_end_time)
                if end < start:
                    # When using relative end and latest_timestamp has been wiped
                    # Start from 10 minutes before the end time
                    start = end - timedelta(minutes=10)
            else:
                end = now_ts
        return {'start_time': start, 'end_time': end}

    def process_records(self, records, sub_api_name):
        """Processes records returned by the API

        Args:
            records (list of dicts): output records from the API stored in a list of dictionaries
            sub_api_name (string): name of the sub_api
        Returns:
            A list of dictionaries
        """
        self.num_records_processed = 0
        self.latest_record_timestamp = ''
        self.record_label = '{0}_data'.format(self.APP_NAME)
        for record in records:
            self.num_records_processed += 1
            # Nest all data returned from the API under the "{APP_NAME}_data" key
            record = {self.record_label: record}

            self.rename_timestamp_key(record)
            # Convert api_timestrings to datetime objects
            record['event_time'] = self.convert_api_timestring_to_iso8601(record['event_time'])
            # Insert the LogFeeder Type and sub-API into each record (to make it easy to sort in the ELK stack)
            record['logfeeder_type'] = self.APP_NAME
            record['logfeeder_subapi'] = sub_api_name
            record['logfeeder_account'] = self.domain
            record['logfeeder_instance'] = self.options.instance_name

            if record['event_time'] > self.latest_record_timestamp:
                self.latest_record_timestamp = record['event_time']

            # Normalize user name information into a single top-level field, to support user-centric dashboards
            if self.USERNAME_FIELD:
                try:
                    # Allow dot notation for nested keys, e.g.
                    # record = {'a': {'b': {'c': 'd'}}}
                    # key = 'a.b' # If all keys hit, we get whatever the resulting type is. In this case, a dict
                    # value => {'c': 'd'}
                    # key = 'a.b.c' # If all keys hit, we get whatever the resulting type is. In this case, a string
                    # value => 'd'
                    # key = 'a.z'  # If the top level key hits, but any sub keys miss, we get None
                    # value => None
                    # It will also automatically take the first element of a list, eg
                    # record = {'a': [{'b': {'c': 'd'}}]} will also produce 'd'
                    org_username_value = reduce(
                        lambda x, y: x.get(y) if type(x) == dict else x[0].get(y),
                        self.USERNAME_FIELD.split('.'),
                        record[self.record_label])
                except (TypeError, AttributeError):
                    # This ^ can happen if you index too far into a primitive type, e.g.
                    # record = {'a': {'b': {'c': 'd'}}}
                    # key = 'a.b.c.d'
                    # Or if any non-leaf node key is a miss followed by another key, e.g.
                    # record = {'a': {'b': {'c': 'd'}}}
                    # key = 'z.z'
                    org_username_value = None

                if org_username_value:
                    record['org_username'] = org_username_value

            yield record

        if not self.options.stateless:
            JsonLogger.log_event(
                'records_fetched', sub_api_name=sub_api_name,
                records_length=self.num_records_processed)

    def sub_api_run(self, api_info, sub_api_name):
        """Runs LogFeeder for a particular sub-api by setting the time range, making the queries, processing the
        records, and outputting the records

        Args:
            api_info (dict): information necessary to make queries to the API, such as authentication credentials
            sub_api_name (string): name of the sub_api
        """
        time_range = self.set_time_range(sub_api_name)


        # Make queries to the API
        latest_event_time = None
        records_generator = self._make_queries_wrapper(
            self.convert_dt_to_api_timestring(time_range['start_time']),
            self.convert_dt_to_api_timestring(time_range['end_time']),
            api_info, sub_api_name,
        )
        for records in records_generator:
            latest_event_time = max(latest_event_time, self._process_and_send_records(records, sub_api_name))

        if not self.options.stateless:
            # Writes the timestamp from the latest record to "{APP_NAME}_{sub_api}_last_timestamp.txt
            if latest_event_time and self.do_write_latest_event_time:
                self.mark_as_completed(latest_event_time, sub_api_name)

    @measure_elapsed_time('records_processed_and_sent')
    def _process_and_send_records(self, records, sub_api_name):
        """Processes the records (if any) and sends them to the output class.
        Writes the event time from the last record.

        Args:
            records (list of dicts): records from the API stored as a list of dicts
            sub_api_name (string): name of the sub_api
        Returns:
            The event time of the latest occurring record
        """
        if records:
            processed_records = self.process_records(records, sub_api_name)
            if not self.options.no_output:
                self.send_records(processed_records, sub_api_name)
            else:
                # if we are not sending to output module, we just dump the results to stdout
                sys.stdout.write('{0}\n'.format(simplejson.dumps(list(records))))
        else:
            JsonLogger.log_event(
                'no_records_returned', sub_api_name=sub_api_name)
            return
        return self.latest_record_timestamp

    def process_commandline_options(self, args=None):
        """
        Read command-line options in to self.options and self.args
        """
        self.parse_options(self.parser)
        return self.parser.parse_args(args=args)[0]

    def start(self):
        """
        This starts up Feeder.
        """
        self.starttime = time.time()
        exit_code = 1

        try:
            self.run()
            exit_code = 0
        except SystemExit as e:
            exit_code = e.code
            raise
        except KeyboardInterrupt:
            exit_code = 'k'
            raise
        except Exception as e:
            import traceback
            traceback.print_exc()
            exit_code = 1
            sys.exit(exit_code)
        finally:
            sys.exit(exit_code)


    def run(self):
        self.parse_config(self.options.config_path)
        JsonLogger.init_logger(
            logfeeder_app=self.APP_NAME, logfeeder_account=self.domain,
            logfeeder_instance=self.options.instance_name)

        # Read API file & initialize time range
        JsonLogger.log_event('feeder_starting')
        api_info = self.read_api_creds(self.api_creds_filepath)

        # Loop through the sub_api's (e.g. the Duo sub_api's are admin, auth, & tele)
        # If there are no actual sub_api's for an API, then self.sub_apis = {self.APP_NAME: True}
        for sub in self.sub_apis:
            # Check to see if this particular sub_api has been selected to run
            if not self.sub_apis[sub]:
                continue
            try:
                self.sub_api_run(api_info, sub)
            except Exception as e:
                self._log_uncaught_exception(e)

    def _log_uncaught_exception(self, exception):
        tb = traceback.format_exc()
        data = {'exception': str(exception), 'traceback': tb}
        JsonLogger.log_event('error', **data)
