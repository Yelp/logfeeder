#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Receives notifications about new OpenDNS log files in an S3 bucket, fetches
the log file and sends each log record to an AWS SQS queue.

OpenDNS logs in S3 are stored in a gzip'd CSV format.

How to configure OpenDNS Log Management in Amazon S3 is explained here:
https://support.opendns.com/entries/60861270

Log Management Export format:
https://support.opendns.com/entries/60037994
"""
from __future__ import absolute_import

import cStringIO
import csv
from itertools import izip

import dateutil.parser
from pytz import utc

from log_feeder.es_feeder import EsOutput
from log_feeder.s3_feeder import S3Feeder
from log_feeder.utils import lock_and_load
from log_feeder.base_feeder import parse_base_feeder_options
from log_feeder.es_feeder import parse_es_options


class UmbrellaFeeder(S3Feeder, EsOutput):

    APP_NAME = 'opendns'
    TIMESTAMP_KEYNAME = 'timestamp'

    field_names = (
        "timestamp",
        "most_granular_identity",
        "identities",
        "internal_ip",
        "external_ip",
        "action",
        "query_type",
        "response_code",
        "domain",
        "categories"
    )

    def transform_records_from_log_file(self, _, log_file_content):
        """Transforms records from CSV format into a list of dictionaries."""
        log_file = cStringIO.StringIO(log_file_content)
        reader = csv.reader(log_file)
        # Filter out debug.opendns.com from logs (domains end in .)
        domain_index = self.field_names.index('domain')
        return [dict(izip(self.field_names, row)) for row in reader if row[domain_index] != "debug.opendns.com."]

    def convert_api_timestring_to_iso8601(self, api_timestring):
        """Timestamp in OpenDNS logs is in UTC."""
        timestamp = dateutil.parser.parse(api_timestring)
        utc_timestamp = timestamp.replace(tzinfo=utc)
        return utc_timestamp.isoformat()


def parse_options():
    parser = parse_base_feeder_options()
    parser = parse_es_options(parser)
    (options, args) = parser.parse_args()
    return options


def main():
    options = parse_options()
    lock_and_load(UmbrellaFeeder(options=options, es_options=options))


if __name__ == "__main__":
    main()
