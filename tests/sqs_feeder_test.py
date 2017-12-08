# -*- coding: utf-8 -*-
from __future__ import absolute_import

from contextlib import nested
from datetime import datetime
from datetime import timedelta
from io import BytesIO

import mock
import pytest
from mock import call
from mock import MagicMock
from mock import patch
from pytz import utc

from log_feeder.onelogin_feeder import OneLoginFeeder
from log_feeder.sqs_feeder import SqsOutput
from log_feeder.utils import get_current_time_with_utc_timezone


def assert_records_equal(expected_records, records_generator):
    for actual_records in records_generator:
        assert expected_records == list(actual_records)


def assert_key_values_found(records, **expected_key_values):
    for key, expected_value in expected_key_values.iteritems():
        found_value = find_value(records, key)
        assert expected_value == found_value


def find_value(records, key):
    if isinstance(records, list):
        for item in records:
            result = find_value(item, key)
            if result:
                return result
    if isinstance(records, dict):
        if key in records:
            return records[key]
        for k, v in records.items():
            if isinstance(v, dict) or isinstance(v, list):
                return find_value(v, key)
    return None


@pytest.fixture
def olf():
    # We can't use the abstract class SqsOutput for testing, so we use one of its subclasses, (e.g. OneLoginFeeder)
    options = MagicMock(
        stateless=False, no_output=False, instance_name='blue', relative_start_time=None,
        relative_end_time=None)
    olf = OneLoginFeeder(options=options)
    olf.domain = 'test_domain.com'
    olf.USERNAME_FIELD = None
    return olf


class SharedTestLogFeeder(object):

    def sub_api_run_tester(
            self, logfeeder_obj, sub_api_name, instance_name,
            make_queries_output, expected_final_records):
        logfeeder_obj.options.no_output = False
        logfeeder_obj.options.instance_name = instance_name
        logfeeder_obj.options.start_time = '2000-01-01T00:00:00Z'
        logfeeder_obj.options.end_time = '2000-01-01T00:00:00Z'
        logfeeder_obj.options.relative_start_time = None
        logfeeder_obj.options.relative_end_time = None
        logfeeder_obj._make_queries_wrapper = MagicMock(return_value=make_queries_output)
        logfeeder_obj.send_records = MagicMock()
        with patch('log_feeder.base_feeder.write_content_to_file'):
            logfeeder_obj.sub_api_run('fake_api_info', sub_api_name)
            call_args = logfeeder_obj.send_records.call_args_list[0][0]
            assert expected_final_records == list(call_args[0])
            assert sub_api_name == call_args[1]


class TestSqs(object):

    def test_parse_config_no_subapis(self, olf):
        main_config_file = BytesIO(
            'logfeeder:\n'
            '  aws_config_filepath: blah\n'
            '  domain: blah\n'
            'onelogin:\n'
            '  file: blah_file\n'
        )
        duo_config_file = BytesIO(
            'api_creds_filepath: blah_file\n'
        )
        with patch('__builtin__.open', side_effect=[main_config_file, duo_config_file]):
            olf.parse_config('mock_config_file')
            assert olf.sub_apis == {olf.APP_NAME: True}

    def test__read_last_timestamp_no_file(self, olf):
        with patch('log_feeder.base_feeder.os.path.exists', return_value=False):
            actual = olf._read_last_time('fake_sub_api')
            expected = None
            assert actual == expected

    def test__read_last_timestamp_with_file(self, olf):
        with patch('log_feeder.base_feeder.os.path.exists', return_value=True):
            with patch('log_feeder.base_feeder.read_content_from_file', return_value='2000-01-01T00:00:00Z'):
                actual = olf._read_last_time(olf.APP_NAME)
                expected = datetime(2000, 1, 1, 0, 0, 0, tzinfo=utc)
                assert actual == expected

    def test_write_latest_event_time(self, olf):
        records_1 = [
            {'event_time': '2000-01-01T09:00:00Z'},
            {'event_time': '2000-01-01T10:00:00Z'},
            {'event_time': '2000-01-01T08:00:00Z'},
        ]
        records_2 = [
            {'event_time': '1999-01-01T09:00:00Z'},
            {'event_time': '1999-01-01T10:00:00Z'},
            {'event_time': '1999-01-01T08:00:00Z'},
        ]
        olf.options.hidden_tag = '_faketag'
        filename = '{0}/{1}_{1}_test_domain.com_last_timestamp{2}.txt'.format(
            olf.log_folder, olf.APP_NAME, olf.options.hidden_tag)
        olf.set_time_range = MagicMock(return_value={'start_time': 'blah', 'end_time': 'blah'})
        olf.convert_dt_to_api_timestring = MagicMock()
        olf._make_queries_wrapper = MagicMock(return_value=[records_1, records_2])
        olf.process_records = MagicMock(side_effect=[records_1, records_2])
        # We need to evaluate the records or write_content wont be called
        olf.send_records = MagicMock(side_effect=lambda x, y: list(x))
        olf.latest_record_timestamp = '2000-01-01T10:00:00Z'
        with patch('log_feeder.base_feeder.write_content_to_file') as mock_write_content:
            olf.sub_api_run('fake_info', olf.APP_NAME)
            call_args = mock_write_content.call_args_list[0][0]
            assert filename == call_args[0]
            assert '2000-01-01T10:00:00Z' == call_args[1]

    def test_skips_write_in_stateless_mode(self, olf):
        records_1 = [
            {'event_time': '2000-01-01T09:00:00Z'},
            {'event_time': '2000-01-01T10:00:00Z'},
            {'event_time': '2000-01-01T08:00:00Z'},
        ]
        records_2 = [
            {'event_time': '1999-01-01T09:00:00Z'},
            {'event_time': '1999-01-01T10:00:00Z'},
            {'event_time': '1999-01-01T08:00:00Z'},
        ]
        # set the feeder to run in stateless mode
        olf.options.stateless = True
        olf.set_time_range = MagicMock(return_value={'start_time': 'blah', 'end_time': 'blah'})
        olf.convert_dt_to_api_timestring = MagicMock()
        olf._make_queries_wrapper = MagicMock(return_value=[records_1, records_2])
        olf.process_records = MagicMock(side_effect=[records_1, records_2])
        olf.send_records = MagicMock()
        with patch('log_feeder.base_feeder.write_content_to_file') as mock_write_content:
            olf.sub_api_run('fake_info', olf.APP_NAME)
            assert mock_write_content.call_count == 0

    @pytest.mark.parametrize("input,expected_call_count", [
        (['a'], 1),
        (['a', 'b', 'c'], 1),
        (['x'] * 100, 10),
        (['a' * 64000], 1),
    ])
    def test__send_sqs_messages(self, olf, input, expected_call_count):
        fake_sqs_queue = MagicMock()
        olf._send_sqs_messages(fake_sqs_queue, input, MagicMock())
        assert fake_sqs_queue.write_batch.call_count == expected_call_count

    def test__send_sqs_messages_too_long_msg(self, olf):
        fake_sqs_queue = MagicMock()
        input = ['{0}"event_time": "2000", "random": "{1}"{2}'.format('{', 'a' * 99999, '}')]
        olf._send_sqs_messages(fake_sqs_queue, input, MagicMock())
        assert fake_sqs_queue.write_batch.call_count == 1
        fake_sqs_queue.write_batch.assert_called_with([[0, input[0][:olf._sqs_msg_chunk_size], 0]])

    def test__send_sqs_batch_too_long_batch(self, olf):
        fake_sqs_queue = MagicMock()
        input = ['{0}"event_time": "2000", "random": "{1}"{2}'.format('{', 'a' * 1024 * 60, '}')] * 5
        olf._send_sqs_messages(fake_sqs_queue, input, MagicMock())
        assert fake_sqs_queue.write_batch.call_count == 2

    def test_set_time_range_two_start_times(self, olf):
        olf.options.start_time = 'two times'
        olf.options.relative_start_time = 'two times'
        with pytest.raises(ValueError):
            olf.set_time_range('blah')

    def test_set_time_range_two_end_times(self, olf):
        olf.options.start_time = '2009-01-01T00:00:00Z'
        olf.options.end_time = 'two times'
        olf.options.relative_end_time = 'two times'
        with pytest.raises(ValueError):
            olf.set_time_range('blah')

    def test_set_time_range_cmd_line(self, olf):
        olf.options.start_time = '2000-01-01T00:00:00Z'
        olf.options.end_time = '2009-01-01T00:00:00Z'
        expected = {
            'start_time': datetime(2000, 1, 1, 0, 0, 0, tzinfo=utc),
            'end_time': datetime(2009, 1, 1, 0, 0, 0, tzinfo=utc),
        }
        actual = olf.set_time_range('blah')
        assert actual == expected

    def test_set_time_range_file(self, olf):
        olf.options.start_time = None
        olf.options.end_time = '2009-01-01T00:00:00Z'
        olf._read_last_time = MagicMock(return_value=datetime(2000, 1, 1, 0, 0, 0, tzinfo=utc))
        expected = {
            'start_time': datetime(2000, 1, 1, 0, 0, 0, tzinfo=utc),
            'end_time': datetime(2009, 1, 1, 0, 0, 0, tzinfo=utc),
        }
        actual = olf.set_time_range('blah')
        assert actual == expected

    def test_set_time_range_file_relative_times(self, olf):
        olf.options.start_time = None
        olf.options.end_time = None
        olf.options.relative_start_time = 100
        olf.options.relative_end_time = 10
        fake_time = datetime(2000, 1, 1, 12, 0, 0, 0)
        with patch('log_feeder.base_feeder.get_current_time_with_utc_timezone', return_value=fake_time):
            actual = olf.set_time_range('blah')
            expected = {'start_time': datetime(2000, 1, 1, 10, 20, 0), 'end_time': datetime(2000, 1, 1, 11, 50, 0)}
            assert actual == expected

    def test_set_time_range_defaults(self, olf):
        olf.options.start_time = None
        olf.options.end_time = None
        olf._read_last_time = MagicMock(return_value=None)
        actual = olf.set_time_range('blah')

        now_ts = datetime.utcnow()
        now_ts = utc.localize(now_ts)
        start_diff = (now_ts - actual['start_time'])
        # Default start time is 10 minutes before the current time
        assert start_diff > timedelta(minutes=9)
        assert start_diff < timedelta(minutes=11)
        # Default end time should be the current time
        assert (now_ts - actual['end_time']) < timedelta(seconds=10)

    def test_process_and_send_records(self):
        """Assert that log feeder processes the record, and calls the send_records method
        if it is not running in no-sqs mode (default behavior)
        """
        records_1 = [
            {'event_time': '2000-01-01T09:00:00Z'},
            {'event_time': '2000-01-01T10:00:00Z'},
            {'event_time': '2000-01-01T08:00:00Z'},
        ]
        records_2 = [
            {'event_time': '1999-01-01T09:00:00Z'},
            {'event_time': '1999-01-01T10:00:00Z'},
            {'event_time': '1999-01-01T08:00:00Z'},
        ]
        with nested(
            patch('log_feeder.sqs_feeder.SqsOutput.process_records'),
            patch('log_feeder.sqs_feeder.SqsOutput.send_records', side_effect=[records_1, records_2]),
            patch('log_feeder.sqs_feeder.SqsOutput.convert_dt_to_api_timestring'),
            patch('log_feeder.sqs_feeder.SqsOutput._make_queries_wrapper', return_value=[records_1, records_2]),
            patch('log_feeder.base_feeder.write_content_to_file'),
            patch(
                'log_feeder.sqs_feeder.SqsOutput.set_time_range',
                MagicMock(return_value={'start_time': 'blah', 'end_time': 'blah'})),
        ) as (mock_process, mock_send, mock_convert, mock_make_queries, write_content_to_file, mock_set_time_range):

            options = MagicMock()
            options.config_path = 'path/config.yaml'
            options.instance_name = 'blue' 
            options.stateless = True
            options.hidden_tag = False 
            options.no_output = False

            feeder = SqsOutput(options=options)
            feeder.sub_api_run('fake_info', 'some_app_name')
            call_list = [call(records_1, 'some_app_name'), call(records_2, 'some_app_name')]
            mock_process.assert_has_calls(call_list)
            call_list = [call(mock.ANY, 'some_app_name'), call(mock.ANY, 'some_app_name')]
            mock_send.assert_has_calls(call_list)

    def test_process_and_print_records(self):
        """Assert that log feeder processes the record, and does not send the records
        if it is not running in no-sqs mode
        """
        records_1 = [
            {'event_time': '2000-01-01T09:00:00Z'},
            {'event_time': '2000-01-01T10:00:00Z'},
            {'event_time': '2000-01-01T08:00:00Z'},
        ]
        records_2 = [
            {'event_time': '1999-01-01T09:00:00Z'},
            {'event_time': '1999-01-01T10:00:00Z'},
            {'event_time': '1999-01-01T08:00:00Z'},
        ]
        with nested(
            patch('log_feeder.sqs_feeder.SqsOutput.process_records'),
            patch('log_feeder.sqs_feeder.SqsOutput.send_records', side_effect=[records_1, records_2]),
            patch('log_feeder.sqs_feeder.SqsOutput.convert_dt_to_api_timestring'),
            patch('log_feeder.sqs_feeder.SqsOutput._make_queries_wrapper', return_value=[records_1, records_2]),
            patch('log_feeder.base_feeder.write_content_to_file'),
            patch(
                'log_feeder.sqs_feeder.SqsOutput.set_time_range',
                MagicMock(return_value={'start_time': 'blah', 'end_time': 'blah'})),
        ) as (mock_process, mock_send, mock_convert, mock_make_queries, write_content_to_file, mock_set_time_range):
            options = MagicMock()
            options.config_path = 'path/config.yaml'
            options.instance_name = 'Test' 
            options.stateless = True
            options.hidden_tag = False 
            options.no_output = True
            feeder = SqsOutput(options=options)
            feeder.sub_api_run('fake_info', 'some_app_name')
            call_list = [call(records_1, 'some_app_name'), call(records_2, 'some_app_name')]
            mock_process.assert_has_calls(call_list)
            assert mock_send.call_count == 0

    def test_process_records_org_username_present_key_found(self, olf):
        olf.remove_keys_from_records = MagicMock()
        olf.USERNAME_FIELD = 'user.name'
        records = [{
            'created-at': '2000-01-01T00:00:00Z',
            'nested': {'a': 'b', 'c': 'd'},
            'type': 'mytype',
            'user': {'name': 'barbie'},
        }]
        actual = olf.process_records(records, 'blah_sub_api')
        expected = [{
            '{0}_data'.format(olf.APP_NAME): {
                'nested': {'a': 'b', 'c': 'd'},
                'type': 'mytype',
                'user': {'name': 'barbie'},
            },
            'logfeeder_type': olf.APP_NAME,
            'logfeeder_subapi': 'blah_sub_api',
            'logfeeder_account': 'test_domain.com',
            'logfeeder_instance': 'blue',
            'org_username': 'barbie',
            'event_time': '2000-01-01T00:00:00+00:00',
        }]
        assert list(actual) == expected

    def test_process_records_org_username_present_array_key_found(self, olf):
        olf.remove_keys_from_records = MagicMock()
        olf.USERNAME_FIELD = 'actors.login'
        records = [{
            'created-at': '2000-01-01T00:00:00Z',
            'actors': [{'login': 'barbie'}],
        }]
        actual = olf.process_records(records, 'blah_sub_api')
        expected = "barbie"
        assert list(actual)[0].get('org_username') == expected

    def test_process_records_org_username_present_lookup_failure_key_not_found(self, olf):
        olf.remove_keys_from_records = MagicMock()
        olf.USERNAME_FIELD = 'user.wrong_name'
        records = [{
            'created-at': '2000-01-01T00:00:00Z',
            'nested': {'a': 'b', 'c': 'd'},
            "type": "mytype",
            'user': {'name': 'barbie'},
        }]
        actual = olf.process_records(records, 'blah_sub_api')
        expected = [{
            '{0}_data'.format(olf.APP_NAME): {
                'nested': {'a': 'b', 'c': 'd'},
                'type': 'mytype',
                'user': {'name': 'barbie'},
            },
            'logfeeder_type': olf.APP_NAME,
            'logfeeder_subapi': 'blah_sub_api',
            'logfeeder_account': 'test_domain.com',
            'logfeeder_instance': 'blue',
            'event_time': '2000-01-01T00:00:00+00:00',
        }]
        assert list(actual) == expected

    def test_process_records_org_username_lookup_failure_type_error(self, olf):
        olf.remove_keys_from_records = MagicMock()
        olf.USERNAME_FIELD = 'completely.wrong.key'
        records = [{
            'created-at': '2000-01-01T00:00:00Z',
            'nested': {'a': 'b', 'c': 'd'},
            'type': 'mytype',
            'user': {'name': 'barbie'},
        }]
        actual = olf.process_records(records, 'blah_sub_api')
        expected = [{
            '{0}_data'.format(olf.APP_NAME): {
                'nested': {'a': 'b', 'c': 'd'},
                'type': 'mytype',
                'user': {'name': 'barbie'},
            },
            'logfeeder_type': olf.APP_NAME,
            'logfeeder_subapi': 'blah_sub_api',
            'logfeeder_account': 'test_domain.com',
            'logfeeder_instance': 'blue',
            'event_time': '2000-01-01T00:00:00+00:00',
        }]
        assert list(actual) == expected

    def test_process_records_org_username_not_present(self, olf):
        olf.remove_keys_from_records = MagicMock()
        records = [{
            'created-at': '2000-01-01T00:00:00Z',
            'nested': {'a': 'b', 'c': 'd'},
            'type': 'mytype',
        }]
        actual = olf.process_records(records, 'blah_sub_api')
        expected = [{
            '{0}_data'.format(olf.APP_NAME): {
                'nested': {'a': 'b', 'c': 'd'},
                'type': 'mytype',
            },
            'logfeeder_type': olf.APP_NAME,
            'logfeeder_subapi': 'blah_sub_api',
            'logfeeder_account': 'test_domain.com',
            'logfeeder_instance': 'blue',
            'event_time': '2000-01-01T00:00:00+00:00',
        }]
        assert list(actual) == expected

    def test_send_records(self, olf):
        olf.aws_config_filepath = 'aws_config'
        input = [{'fake': 'record'}]
        fake_sqs_queue = 'sqs_queue'
        with patch('log_feeder.sqs_feeder.get_sqs_queue', return_value=fake_sqs_queue):
            with patch('log_feeder.sqs_feeder.SqsOutput._send_sqs_messages') as fake_sender:
                olf.send_records(input, olf.APP_NAME)
                call_args = fake_sender.call_args_list[0][0]
                assert fake_sqs_queue == call_args[0]
                assert ['{"fake": "record"}'] == list(call_args[1])

    def test_sub_api_run_no_records(self, olf):
        olf.set_time_range = MagicMock(return_value={
            'start_time': get_current_time_with_utc_timezone(),
            'end_time': get_current_time_with_utc_timezone()
        })
        olf._make_queries_wrapper = MagicMock(return_value=iter([]))
        expected = None
        actual = olf.sub_api_run('blah_api_info', 'blah_sub_api')
        assert expected == actual

    def test_run_onelogin(self, olf):
        olf.parse_config = MagicMock()
        olf.sub_api_run = MagicMock()
        olf.api_creds_filepath = MagicMock()
        olf.read_api_creds = MagicMock(return_value='api_creds')
        olf.sub_apis = {olf.APP_NAME: True}
        olf.run()
        olf.sub_api_run.assert_called_with('api_creds', olf.APP_NAME)
