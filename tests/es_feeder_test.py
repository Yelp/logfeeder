# -*- coding: utf-8 -*-
from __future__ import absolute_import

from datetime import datetime

import pytest
import simplejson
from mock import ANY
from mock import MagicMock
from mock import patch

from log_feeder.es_feeder import EsOutput


def assert_records_equal(expected_records, records_generator):
    for actual_records in records_generator:
        assert expected_records == list(actual_records)


@pytest.fixture
def esf():
    es_options = MagicMock()
    es_options.hostnames = ['localhost']
    es_options.port = 80
    es_options.chunksize = 10000
    es_options.in_order = True
    es_options.index = 'test-%V'

    options = MagicMock()
    options.config_path = 'path/config.yaml'
    options.instance_name = 'blue'
    options.start_time = None
    options.end_time = None
    options.relative_start_time = None
    options.relative_end_time = None 
    options.stateless = False
    options.hidden_tag = False
    options.no_output = False

    esf = EsOutput(options=options, es_options=es_options)
    esf.APP_NAME = 'test_app'
    esf.domain = 'test_domain.com'
    return esf


class TestEs(object):

    def test_a_record(self, esf):
        mock_es = MagicMock()
        with patch('log_feeder.es_feeder.Elasticsearch', return_value=mock_es):
            with patch('log_feeder.es_feeder.datetime') as mock_dt:
                with patch('__builtin__.open'):
                    mock_dt.utcnow.return_value = datetime(2015, 10, 1)
                    records = [{'event_time': '2015-09-30T00:00:00Z', 'foo': 'bar'}]
                    esf.send_records(records, 'test_api')
                    call_args = mock_es.bulk.call_args_list
                    print call_args
                    assert 'body' in call_args[0][1]
                    assert 'index' in call_args[0][1]
                    assert call_args[0][1]['index'] == 'test-40'
                    lines = call_args[0][1]['body'].split('\n')
                    command = simplejson.loads(lines[0])
                    record = simplejson.loads(lines[1])
                    assert command['create']['_index'] == 'test-40'
                    assert record['@ingestionTime'] == '2015-10-01T00:00:00Z'
                    assert record['foo'] == 'bar'
                    assert record['@timestamp'] == records[0]['event_time']
                    assert record['@time_delta_seconds'] == 86400

    def test_marked_as_completed(self, esf):
        mock_es = MagicMock()
        with patch('log_feeder.es_feeder.Elasticsearch', return_value=mock_es):
            with patch('log_feeder.es_feeder.datetime') as mock_dt:
                with patch.object(esf, 'mark_as_completed') as mock_completed:
                    mock_dt.utcnow.return_value = datetime(2015, 10, 1)

                    records = [{'event_time': '2015-09-30T00:00:00Z', 'foo': 'bar'}]
                    esf.send_records(records, 'test_api')
                    mock_completed.assert_called_once_with(ANY, 'test_api')

    def test_different_indices(self, esf):
        esf.in_order = False
        mock_es = MagicMock()
        with patch('log_feeder.es_feeder.Elasticsearch', return_value=mock_es):
            with patch('log_feeder.es_feeder.datetime') as mock_dt:
                with patch('__builtin__.open'):
                    mock_dt.utcnow.return_value = datetime(2015, 10, 1)

                    records = [{'event_time': '2015-09-20T00:00:00Z', 'foo': 'bar'},
                               {'event_time': '2015-09-30T00:00:00Z', 'foo': 'bar'}]
                    esf.send_records(records, 'test_api')
                    call_args = mock_es.bulk.call_args_list

                    # Calls might be in any order
                    indices = ['test-38', 'test-40']

                    # Record 1
                    lines = call_args[0][1]['body'].split('\n')
                    command = simplejson.loads(lines[0])
                    assert command['create']['_index'] in indices
                    indices.remove(command['create']['_index'])

                    # Record 2
                    lines = call_args[1][1]['body'].split('\n')
                    command = simplejson.loads(lines[0])
                    assert command['create']['_index'] in indices

    def test_set_id(self, esf):
        esf.in_order = False
        mock_es = MagicMock()
        with patch('log_feeder.es_feeder.Elasticsearch', return_value=mock_es):
            with patch('log_feeder.es_feeder.datetime') as mock_dt:
                with patch('__builtin__.open'):
                    mock_dt.utcnow.return_value = datetime(2015, 10, 1)

                    records = [{'event_time': '2015-09-20T00:00:00Z',
                                'test_app_data': {'foo': 'bar', '_id': 'abcdefgh'}}]
                    esf.send_records(records, 'test_api')
                    call_args = mock_es.bulk.call_args_list
                    bulk_header = call_args[0][1]['body'].split('\n')[0]
                    assert simplejson.loads(bulk_header)['create']['_id'] == 'abcdefgh'

    def test_fingerprint(self, esf):
        """ Tests that the same _id is generated even when time changes """
        mock_es = MagicMock()
        with patch('log_feeder.es_feeder.Elasticsearch', return_value=mock_es):
            with patch('__builtin__.open'):

                records = [{'event_time': '2015-09-20T00:00:00Z',
                            'test_app_data': {'foo': 'bar'}}]

                # Emit the first event and record the _id
                esf.send_records(records, 'test_api')
                call_args = mock_es.bulk.call_args_list
                bulk_header = call_args[0][1]['body'].split('\n')[0]
                bulk_body1 = call_args[0][1]['body'].split('\n')[1]
                first_id = simplejson.loads(bulk_header)['create']['_id']

                # Emit the second event and record the _id
                esf.send_records(records, 'test_api')
                call_args = mock_es.bulk.call_args_list
                bulk_header = call_args[1][1]['body'].split('\n')[0]
                bulk_body2 = call_args[1][1]['body'].split('\n')[1]
                second_id = simplejson.loads(bulk_header)['create']['_id']

                assert first_id == second_id
                # The two documents should be different (because of ingestionTime)
                parsed_body1 = simplejson.loads(bulk_body1)
                parsed_body2 = simplejson.loads(bulk_body2)
                assert parsed_body1['@ingestionTime'] != parsed_body2['@ingestionTime']
                parsed_body1.pop('@ingestionTime')
                parsed_body2.pop('@ingestionTime')
                assert simplejson.dumps(parsed_body1) == simplejson.dumps(parsed_body2)
