#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Reads events from the OneLogin API and sends them to an AWS SQS Queue
"""
from __future__ import absolute_import

import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO

import dateutil.parser
import pycurl

from log_feeder.onelogin_events import event_id_to_description
from log_feeder.sqs_feeder import SqsOutput
from log_feeder.utils import format_dt_as_iso8601
from log_feeder.utils import lock_and_load
from log_feeder.utils import read_yaml_file
from log_feeder.base_feeder import parse_base_feeder_options

class OneLoginFeeder(SqsOutput):

    """Reads data from OneLogin API and feeds it to an AWS SQS Queue"""
    APP_NAME = 'onelogin'
    TIMESTAMP_KEYNAME = 'created-at'

    def read_api_creds(self, onelogin_info_file):
        """Read OneLogin credentials necessary to access activity data from the OneLogin API

        Args:
            config_file: file containing OneLogin credentials
        Returns:
            OneLogin API Key in a dictionary
        """
        yaml_content = read_yaml_file(onelogin_info_file, ['api_key'])
        return {'onelogin_key': yaml_content['api_key']}

    def _make_queries(self, start_time, end_time, api_info_dict, sub_api_name):
        """Makes queries to the OneLogin API for records in the specified time range

        Args:
            start_time (string): starting time for log ingestion as a iso8601 time string
            end_time (string): ending time for log ingestion as a iso8601 time string
            api_info_dict: a dictionary containing the OneLogin API Key
            sub_api_name (string): the name of the sub_api to be called (if the API has sub_apis)
        Yields:
            An enumerable of dicts containing data from OneLogin API
        """
        self.event_id_to_description = event_id_to_description
        start_time = dateutil.parser.parse(start_time)
        end_time = dateutil.parser.parse(end_time)
        query_results = []
        tries = 1
        page_num = 1
        earliest_entry = None
        pycurl_obj = pycurl.Curl()
        pycurl_obj.setopt(pycurl_obj.USERPWD, '{0}:x'.format(api_info_dict['onelogin_key']))

        while earliest_entry is None or earliest_entry >= start_time:
            # The OneLogin API returns the most recent entries first and the oldest entries last
            (status_code, xml_results) = self.query_page(pycurl_obj, page_num)
            if status_code != 200:
                if tries >= 5:
                    raise ValueError(
                        'Failed to fetch OneLogin data for the time range {0} to {1} five consecutive times. An HTTP '
                        'status code other than 200 was returned 5 consecutive times. Here is the last page '
                        'returned by the OneLogin API: {2}'.format(earliest_entry, start_time, xml_results)
                    )
                tries += 1
                continue

            if '<nil-classes type="array"/>' in xml_results:
                self.log.warning('No results were returned from OneLogin for page {0}. The start time, '
                                 '{1}, might be too early and the OneLogin logs do not appear to go back '
                                 'that far'.format(page_num, start_time))
                break

            # Find the timestamp of the earliest entry in the returned XML page
            found = False
            earliest_entry = None
            root = ET.fromstring(xml_results)
            for entry in root.getchildren()[-1].getchildren():
                if self.TIMESTAMP_KEYNAME == entry.tag:
                    earliest_entry = dateutil.parser.parse(entry.text)
                    found = True
                    break
            if not found:
                raise ET.ParseError("Cannot find created-at tag in this xml_page: {0}".format(xml_results))

            if earliest_entry <= end_time:
                # If True, then at least some of the entries fall in the desired time range
                for event in root.getchildren():
                    record_data = {}
                    for entry in event.getchildren():
                        outside_of_time_range = False
                        record_data[entry.tag] = entry.text
                        if entry.tag == self.TIMESTAMP_KEYNAME:
                            time_entry = dateutil.parser.parse(entry.text)
                            if time_entry < start_time:
                                pycurl_obj.close()
                                return self.add_event_descriptions(query_results)
                            elif time_entry > end_time:
                                outside_of_time_range = True
                                break
                    if not outside_of_time_range:
                        query_results.append(record_data)
            page_num += 1
            tries = 1
        pycurl_obj.close()
        return self.add_event_descriptions(query_results)

    def query_page(self, pycurl_obj, page_num):
        """ Calls the OneLogin API to fetch a page of API results corresponding to page_num

        Args:
            pycurl_obj: a pycurl object used to make the request to the API.
            page_num: the API page to fetch. Low page numbers (e.g. page=1) have the most recent entries.
        Returns:
            A tuple of the HTTP status code and the XML content of the page returned by the API
        """
        buffer = BytesIO()
        pycurl_obj.setopt(pycurl_obj.URL, 'https://app.onelogin.com/api/v1/events?page={0}'.format(page_num))
        pycurl_obj.setopt(pycurl_obj.WRITEFUNCTION, buffer.write)
        self.log.debug('OneLogin Page #{0} requested at {1}'.format(page_num, datetime.utcnow()))
        pycurl_obj.perform()
        self.log.debug('OneLogin Page #{0} received at {1}'.format(page_num, datetime.utcnow()))
        return (pycurl_obj.getinfo(pycurl.HTTP_CODE), buffer.getvalue())

    def add_event_descriptions(self, records):
        """ Adds an event type description to each record, based on its event type it (integer)

        Args:
            records (list of dicts): Records returned from the OneLogin API
        Returns:
            Records with the event-type-description field added
        """
        for rec in records:
            event_description = self.event_id_to_description.get(rec['event-type-id'])
            if event_description:
                rec['event-type-description'] = event_description
            else:
                rec['event-type-description'] = 'No Event Description Provided by OneLogin'
        return records

    def convert_dt_to_api_timestring(self, datetime_obj):
        """Converts a datetime object to an iso8601 time string

        Args:
            datetime_obj (datetime): A datetime object
        Returns:
            A iso8601 time string
        """
        return format_dt_as_iso8601(datetime_obj)

    def convert_api_timestring_to_iso8601(self, api_timestring):
        """Converts a time string output by the API to a iso8601 time string

        Args:
            api_timestring (string): A time string output by the API
        Returns:
            A time string in the iso8601 format
        """
        return format_dt_as_iso8601(dateutil.parser.parse(api_timestring))


def parse_options():
    parser = parse_base_feeder_options()
    (options, args) = parser.parse_args()
    return options

def main():
    options = parse_options()
    lock_and_load(OneLoginFeeder(options=options))

if __name__ == "__main__":
    main()
