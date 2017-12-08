# -*- coding: utf-8 -*-
from __future__ import absolute_import

import pytest
import simplejson
from mock import MagicMock

from log_feeder.umbrella_feeder import UmbrellaFeeder
from log_feeder.utils import read_content_from_file
from tests.s3_feeder_test import add_s3_feeder_config
from tests.s3_feeder_test import run_make_queries
from tests.s3_feeder_test import SharedTestS3Feeder


TEST_DATA_DIR_PATH = 'tests/data/opendns'


def _read_opendns_log_records(opendns_log_records_file_name):
    opendns_log_records_file_path = '{0}/{1}'.format(
        TEST_DATA_DIR_PATH, opendns_log_records_file_name)
    with open(opendns_log_records_file_path) as fp:
        opendns_log_records = simplejson.load(fp)
    return opendns_log_records


@pytest.fixture
def umbrella_feeder():
    options = MagicMock()
    options.instance_name = 'test-opendns-instance'

    es_options = MagicMock()
    es_options.hostnames = ['localhost']
    es_options.port = 80
    es_options.chunksize = 10000
    es_options.in_order = True,
    es_options.index = 'the_es_index'
    feeder = UmbrellaFeeder(options=options, es_options=es_options)
    feeder.domain = 'test-opendns-account'
    add_s3_feeder_config(feeder)
    return feeder


class TestSharedUmbrellaFeeder(SharedTestS3Feeder):

    @pytest.mark.parametrize(
        'opendns_log_records_file_name,expected_final_records_file_name', [
            ('opendns_single_log_record.json',
                'opendns_single_log_logstash_input.json'),
            ('opendns_multiple_logs_records.json',
                'opendns_multiple_logs_logstash_input.json'),
        ]
    )
    def test_sub_api_run(self, umbrella_feeder, opendns_log_records_file_name,
                         expected_final_records_file_name):
        opendns_log_records = _read_opendns_log_records(
            opendns_log_records_file_name)
        make_queries_output = [opendns_log_records]

        self.sub_api_run_tester(
            umbrella_feeder, make_queries_output, TEST_DATA_DIR_PATH,
            expected_final_records_file_name)


class TestUmbrellaFeeder(object):

    @pytest.mark.parametrize(
        's3_event_notifications_file_name,opendns_log_file_name', [
            ('s3_event_single_notification.json', 'opendns_single_log.csv'),
            ('s3_event_notifications.json', 'opendns_logs.csv'),
        ]
    )
    def test_make_queries(self, umbrella_feeder,
                          s3_event_notifications_file_name,
                          opendns_log_file_name):
        run_make_queries(
            umbrella_feeder, TEST_DATA_DIR_PATH,
            s3_event_notifications_file_name, opendns_log_file_name,
            'bucket_store')

    @pytest.mark.parametrize(
        'opendns_log_file_name,opendns_log_records_file_name', [
            ('opendns_single_log.csv', 'opendns_single_log_record.json'),
            ('opendns_logs.csv', 'opendns_multiple_logs_records.json'),
        ]
    )
    def test_transform_records_from_log_file(self, umbrella_feeder,
                                             opendns_log_file_name,
                                             opendns_log_records_file_name):
        opendns_log_file_path = '{0}/{1}'.format(
            TEST_DATA_DIR_PATH, opendns_log_file_name)
        opendns_log_file = read_content_from_file(opendns_log_file_path)

        expected_opendns_log_records = _read_opendns_log_records(
            opendns_log_records_file_name)
        actual_opendns_log_records = \
            umbrella_feeder.transform_records_from_log_file(None, opendns_log_file)

        assert expected_opendns_log_records == actual_opendns_log_records

    def test_convert_api_timestring_to_iso8601(self, umbrella_feeder):
        api_timestamp = '2015-08-17 23:52:31'
        iso8601_timestamp = umbrella_feeder.convert_api_timestring_to_iso8601(
            api_timestamp)
        assert '2015-08-17T23:52:31+00:00' == iso8601_timestamp
