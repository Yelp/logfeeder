#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Reads events from the Duo Admin API and sends them to an AWS SQS Queue
"""
from __future__ import absolute_import

from datetime import datetime

import duo_client
import simplejson
from pytz import utc

from log_feeder.sqs_feeder import SqsOutput
from log_feeder.utils import datetime_to_unixtime
from log_feeder.utils import format_dt_as_iso8601
from log_feeder.utils import lock_and_load
from log_feeder.utils import read_yaml_file
from log_feeder.base_feeder import parse_base_feeder_options

class DuoFeeder(SqsOutput):

    """Reads Activity Data from the Duo API and feeds it to an AWS SQS Queue"""

    def __init__(self, **kwargs):
        super(DuoFeeder, self).__init__(**kwargs)

    APP_NAME = 'duo'
    TIMESTAMP_KEYNAME = 'timestamp'
    ADMIN_SUBAPI = 'admin'
    AUTH_SUBAPI = 'auth'
    TELE_SUBAPI = 'tele'
    # Duo returns at most 1000 records per API call
    MAX_EVENTS_PER_CALL = 1000

    def read_api_creds(self, config_file):
        """Read Duo credentials necessary to access activity data from the Duo API

        Args:
            config_file: file containing duo credentials
        Returns:
            Duo credentials object in a dictionary, or None on failure
        """
        yaml_content = read_yaml_file(config_file, ['integration_key', 'secret_key', 'api_hostname'])
        admin_api = duo_client.Admin(
            ikey=yaml_content['integration_key'],
            skey=yaml_content['secret_key'],
            host=yaml_content['api_hostname'])
        return {'admin_api': admin_api}

    def _make_queries(self, start_time, end_time, api_info_dict, sub_api_name):
        """Makes queries to the Duo API for records in the specified time range

        Args:
            start_time (string): starting time (as a string recognized by the API) for log ingestion
            end_time (string): ending time (as a string recognized by the API) for log ingestion
            api_info_dict: a dictionary containing a Duo API access object
            sub_api_name (string): the name of the sub_api to be called (if the API has sub_apis)
        Yields:
            An enumerable of dicts containing data from Duo API
        """
        admin_api = api_info_dict['admin_api']
        duo_get_method = None
        if sub_api_name == self.ADMIN_SUBAPI:
            duo_get_method = admin_api.get_administrator_log
        elif sub_api_name == self.AUTH_SUBAPI:
            duo_get_method = admin_api.get_authentication_log
        elif sub_api_name == self.TELE_SUBAPI:
            duo_get_method = admin_api.get_telephony_log

        # Call the Duo sub-API
        query_results = self._get_logs(duo_get_method, start_time, end_time)
        for qr in query_results:
            if 'description' in qr and qr['description']:
                try:
                    # Try to convert string representation of dictionary for key='description' to an actual dictionary
                    # so that we can perform some transformations on the dictionary structure
                    qr['description'] = simplejson.loads(qr['description'])
                    self.replace_hardtoken_keys(qr['description'], qr['timestamp'])
                except simplejson.decoder.JSONDecodeError as e:
                    # According to duo documentation (https://duo.com/docs/adminapi#administrator-logs),
                    # the description field "is intended only to summarize the change, not to be deserialized."
                    # So if deserialization fails, log the error and leave the field as it was when we received it
                    self.log.error('Failed to deserialize description field {0}. Exception: {1}'.format(
                        qr['description'], e))
            yield qr

    def _get_logs(self, log_fetcher, unix_start_time, unix_end_time):
        """ Gets Duo logs using a passed Duo API method

        Args:
            log_fetcher: A passed Duo API method (e.g. "get_administrator_log")
            unix_start_time: Start of time range for getting logs (in Unix format)
            unix_end_time: End of time range for getting logs (in Unix format)
        Returns:
            A list containing the aggregate results of the Duo API queries
        """
        MAX_TRIES = 5
        num_duo_calls = 0
        qualifying_events = []
        while True:
            # Attempt to call the Duo API once
            for attempt in range(MAX_TRIES):
                # Get more events and add their contents to the query_results
                self.rate_limiter.delay_next_calls(1)
                try:
                    num_duo_calls += 1
                    event_batch = log_fetcher(mintime=unix_start_time) or []
                except RuntimeError as e:
                    if attempt == (MAX_TRIES - 1):
                        self.log.error('Failed to call the Duo API {0} consecutive times. Giving up.'.format(
                            MAX_TRIES))
                        return []
                    self.log.warning('Caught the following error from the Duo API: {0}'.format(e))
                else:
                    break
            # Process the events in the returned results from the API
            for event in event_batch:
                if int(event['timestamp']) > unix_end_time:
                    break
                qualifying_events.append(event)
            # Set the next start_time to (1 + the timestamp of the last event in the current loop)
            if len(event_batch) > 0:
                unix_start_time = int(event_batch[-1][self.TIMESTAMP_KEYNAME]) + 1

            # End loop when less than MAX_EVENTS_PER_CALL are returned by the API, or no entries in the event_batch,
            # or when the last event's timestamp exceeds the specified end time
            if self.MAX_EVENTS_PER_CALL != len(event_batch) or len(event_batch) == 0 or int(
                    event_batch[-1][self.TIMESTAMP_KEYNAME]) > unix_end_time:
                break

        self.log.debug("{0} calls made to the Duo API".format(num_duo_calls))
        return qualifying_events

    def replace_hardtoken_keys(self, rec_desc, timestamp):
        """Replaces the hardtoken key-names so that they are consistent.

        By default, the hardtoken key-names take on numerous values (e.g.: hardtoken-d1-350386, hardtoken-yk-239753)
        These key-names contain the platform name and serial number, which is also stored as dictionary values (see
        example input below), so it's unnecessary to store them as a dictionary key and makes it difficult to filter
        on these key-values in Kibana.

        This method replaces the key-names with sequential names (e.g.: hardtoken_1, hardtoken_2, etc.)

        Example Input:
        description = {
            'hardtokens': {
                'hardtoken-ab-1234': {
                    'serialnumber': '1234',
                    'platform': 'ab',
                    'totp_step': '',
                },
                'hardtoken-cd-5678': {
                    'serialnumber': '5678',
                    'platform': 'cd',
                    'totp_step': '',
                }
            }
        }

        Example Result:
        description = {
            'hardtoken_1': {
                'serialnumber': '1234',
                'platform': 'ab',
                'totp_step': '',
            },
            'hardtoken_2': {
                'serialnumber': '5678',
                'platform': 'cd',
                'totp_step': '',
            }
        }

        Args:
            res_desc (dict): a dictionary of "description" data for a Duo API record
            timestamp (int): A unix timestamp for the Duo API record
        """
        if 'hardtokens' not in rec_desc:
            return
        hardtokens = rec_desc.get('hardtokens')
        if not hardtokens:
            return
        count = 1
        for token_key in hardtokens:
            # Split the key-name for the hardtoken
            try:
                __, platform, serial = token_key.split('-')
            except ValueError:
                self.log.warn('hardtoken key-name not formatted as expected. Skipping reformat for record '
                              'at timestamp {0} with the key-name {1}'.format(timestamp, token_key))
                return
            # Get the dictionary for the hardtoken (ht)
            new_token = {}
            new_token['platform'] = platform
            new_token['serialnumber'] = serial

            token_dict = hardtokens.get(token_key)
            if token_dict is None:
                self.log.warn('hardtoken value is None at timestamp {0} with the key-name {1}'.format(
                    timestamp, token_key))
            # If possible, verify that the platform and serialnumber in platform key-name match the dictionary values
            else:
                if 'platform' in token_dict and platform != token_dict['platform']:
                    raise ValueError(
                        'The platform name in the keyname "{0}" is not equal to the value of record["description"]'
                        '["hardtokens"]["{0}"]["serialnumber"] (which equals {1}). This occurs at the '
                        'time {2}'.format(token_key, token_dict['platform'], timestamp))
                if 'serialnumber' in token_dict and serial != token_dict['serialnumber']:
                    raise ValueError(
                        'The serial # in the keyname "{0}" is not equal to the value of record["description"]'
                        '["hardtokens"]["{0}"]["serialnumber"] (which equals {1}). This occurs at the '
                        'time {2}'.format(token_key, token_dict['serialnumber'], timestamp))

                # Create a new dict for the hardtoken information
                for keyname in token_dict:
                    new_token[keyname] = token_dict[keyname]

            rec_desc['hardtoken_{0}'.format(count)] = new_token
            count += 1
        del rec_desc['hardtokens']

    def convert_dt_to_api_timestring(self, datetime_obj):
        """Converts a datetime object to a time string accepted by the API

        Args:
            datetime_obj (datetime): A datetime object
        Returns:
            A string representing the datetime that is accepted by the API
        """
        return datetime_to_unixtime(datetime_obj)

    def convert_api_timestring_to_iso8601(self, api_timestring):
        """Converts a time string output by the API to a iso8601 time string

        Args:
            api_timestring (string): A time string output by the API
        Returns:
            A time string in the iso8601 format
        """
        return format_dt_as_iso8601(datetime.utcfromtimestamp(api_timestring).replace(tzinfo=utc))


def parse_options():
    parser = parse_base_feeder_options()
    (options, args) = parser.parse_args()
    return options

def main():
    options = parse_options()
    lock_and_load(DuoFeeder(options=options))

if __name__ == "__main__":
    main()
