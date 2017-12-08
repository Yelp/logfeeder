# -*- coding: utf-8 -*-
from __future__ import absolute_import

from collections import OrderedDict
from datetime import datetime

import dateutil.parser
import pytest
from mock import MagicMock
from mock import patch

from log_feeder.duo_feeder import DuoFeeder
from log_feeder.utils import datetime_to_unixtime
from tests.sqs_feeder_test import assert_records_equal
from tests.sqs_feeder_test import SharedTestLogFeeder


@pytest.fixture
def df_test_obj():
    options = MagicMock()
    options.config_path = 'path/config.yaml'
    options.instance_name = 'blue'
    options.stateless = True
    options.hidden_tag = False
    options.no_output = False

    df = DuoFeeder(options=options)
    df.rate_limiter = MagicMock()
    df.rate_limiter.delay_next_calls = lambda x: None
    df.domain = 'test_domain.com'
    return df


class TestSharedDuo(SharedTestLogFeeder):

    def test_sub_api_run_admin(self, df_test_obj):

        df_test_obj.write_latest_event_time = MagicMock()
        make_queries_output = [[{
            'action': 1, 'eventtype': 2, 'host': 3,
            'description': {
                'uname': 'jdoe',
                'phones': {
                    'phone1': {
                        'number': 4,
                        'type': 5,
                    }
                }
            },
            'object': 6,
            'username': 7,
            'timestamp': 1234567890,
        }]]
        expected = [{
            '{0}_data'.format(df_test_obj.APP_NAME): {
                'action': 1, 'eventtype': 2, 'host': 3,
                'description': {
                    'uname': 'jdoe',
                    'phones': {
                        'phone1': {
                            'number': 4,
                            'type': 5,
                        }
                    }
                },
                'object': 6,
                'username': 7,
            },
            'logfeeder_type': df_test_obj.APP_NAME,
            'logfeeder_subapi': df_test_obj.ADMIN_SUBAPI,
            'logfeeder_account': 'test_domain.com',
            'logfeeder_instance': 'violet',
            'event_time': '2009-02-13T23:31:30+00:00',
        }]
        self.sub_api_run_tester(df_test_obj, df_test_obj.ADMIN_SUBAPI, 'violet', make_queries_output, expected)

    def test_sub_api_run_auth(self, df_test_obj):

        df_test_obj.write_last_time = MagicMock()
        make_queries_output = [[{
            'new_enrollment': 1, 'factor': 2, 'integration': 3, 'reason': 4, 'result': 5, 'device': 6, 'eventtype': 7,
            'username': 8, 'ip': 9, 'host': 10, 'timestamp': 1234567890,
        }]]
        expected = [{
            '{0}_data'.format(df_test_obj.APP_NAME): {
                'new_enrollment': 1, 'factor': 2, 'integration': 3, 'reason': 4, 'result': 5, 'device': 6,
                'eventtype': 7, 'username': 8, 'ip': 9, 'host': 10,
            },
            'event_time': '2009-02-13T23:31:30+00:00',
            'logfeeder_type': 'duo',
            'logfeeder_subapi': df_test_obj.AUTH_SUBAPI,
            'logfeeder_account': 'test_domain.com',
            'logfeeder_instance': 'black',
        }]
        self.sub_api_run_tester(df_test_obj, df_test_obj.AUTH_SUBAPI, 'black', make_queries_output, expected)

    def test_sub_api_run_tele(self, df_test_obj):

        df_test_obj.write_last_time = MagicMock()
        make_queries_output = [[{
            'context': 1, 'credits': 2, 'timestamp': 1234567890, 'eventtype': 4, 'host': 5, 'type': 6,
            'phone': 7,
        }]]
        expected = [{
            '{0}_data'.format(df_test_obj.APP_NAME): {
                'context': 1, 'credits': 2, 'eventtype': 4,
                'host': 5, 'type': 6, 'phone': 7,
            },
            'logfeeder_type': 'duo',
            'event_time': '2009-02-13T23:31:30+00:00',
            'logfeeder_subapi': df_test_obj.TELE_SUBAPI,
            'logfeeder_account': 'test_domain.com',
            'logfeeder_instance': 'white',
        }]
        self.sub_api_run_tester(df_test_obj, df_test_obj.TELE_SUBAPI, 'white', make_queries_output, expected)


class TestDuo(object):

    test_unix_time = datetime_to_unixtime(dateutil.parser.parse('2000-01-01T00:00:00Z'))

    def test__read_api_info_correct_keys(self, df_test_obj):
        # Tests an api_file with correct keys, but invalid values.
        with patch('log_feeder.duo_feeder.read_yaml_file', return_value={
            'integration_key': '123',
            'secret_key': 'abc',
            'api_hostname': 'my_name',
            'random_key': 'random_value',
        }):
            duo_admin = df_test_obj.read_api_creds('fake_filename')
            actual = duo_admin['admin_api']
            assert actual.ikey == '123'
            assert actual.skey == 'abc'
            assert actual.host == 'my_name'

    def test__make_queries(self, df_test_obj):
        for sub_api in [df_test_obj.ADMIN_SUBAPI, df_test_obj.AUTH_SUBAPI, df_test_obj.TELE_SUBAPI]:
            query_results = [{'description': '{"name": "john", "id": "123"}', 'timestamp': 99}]
            df_test_obj._get_logs = MagicMock(return_value=query_results)
            records_generator = df_test_obj._make_queries_wrapper(
                'start_time', 'end_time', {'admin_api': MagicMock()}, sub_api)
            expected_records = [{'description': {'name': 'john', 'id': '123'}, 'timestamp': 99}]
            assert_records_equal(expected_records, records_generator)

    def test__make_queries_description_is_none(self, df_test_obj):
        for sub_api in [df_test_obj.ADMIN_SUBAPI, df_test_obj.AUTH_SUBAPI, df_test_obj.TELE_SUBAPI]:
            query_results = [{'description': None, 'timestamp': 99}]
            df_test_obj._get_logs = MagicMock(return_value=query_results)
            records_generator = df_test_obj._make_queries_wrapper(
                'start_time', 'end_time', {'admin_api': MagicMock()}, sub_api)
            expected_records = [{'description': None, 'timestamp': 99}]
            assert_records_equal(expected_records, records_generator)

    def test__make_queries_description_fails_deserialization(self, df_test_obj):
        for sub_api in [df_test_obj.ADMIN_SUBAPI, df_test_obj.AUTH_SUBAPI, df_test_obj.TELE_SUBAPI]:
            query_results = [{'description': 'this will not deserialize', 'timestamp': 99}]
            df_test_obj._get_logs = MagicMock(return_value=query_results)
            records_generator = df_test_obj._make_queries_wrapper(
                'start_time', 'end_time', {'admin_api': MagicMock()}, sub_api)
            expected_records = [{'description': 'this will not deserialize', 'timestamp': 99}]
            assert_records_equal(expected_records, records_generator)

    def test__get_logs_6_of_10_events(self, df_test_obj):
        # Events returned = 10, six of which don't exceed the end_time
        base_time = TestDuo.test_unix_time - 5
        mock_events_1 = [{'timestamp': i + base_time, 'eid': i} for i in range(10)]
        mock_log_fetcher = MagicMock(return_value=mock_events_1)
        assert mock_events_1[:6] == df_test_obj._get_logs(
            mock_log_fetcher, TestDuo.test_unix_time, TestDuo.test_unix_time
        )
        mock_log_fetcher.assert_called_once_with(mintime=TestDuo.test_unix_time)

    def test__get_logs_2010_events(self, df_test_obj):
        # Events returned = 2010, all of which don't exceed the end_time
        base_time = TestDuo.test_unix_time - 3000
        mock_events_2 = [{'timestamp': i + base_time, 'eid': i} for i in range(2010)]
        mock_log_fetcher = MagicMock(side_effect=[
            mock_events_2[:1000], mock_events_2[1000:2000], mock_events_2[2000:], StopIteration
        ])
        assert mock_events_2 == df_test_obj._get_logs(
            mock_log_fetcher, TestDuo.test_unix_time,
            TestDuo.test_unix_time)
        assert mock_log_fetcher.call_count == 3

    def test__get_logs_no_events(self, df_test_obj):
        # No events returned (empty list)
        mock_log_fetcher = MagicMock(return_value=[])
        actual = df_test_obj._get_logs(mock_log_fetcher, TestDuo.test_unix_time, TestDuo.test_unix_time)
        expected = []
        assert actual == expected

    def test__get_logs_runtime_errors(self, df_test_obj):
        # RuntimeErrors are raised
        mock_log_fetcher = MagicMock(side_effect=[RuntimeError] * 5)
        actual = df_test_obj._get_logs(mock_log_fetcher, TestDuo.test_unix_time, TestDuo.test_unix_time)
        expected = []
        assert actual == expected

    def test_convert_dt_to_api_timestring(self, df_test_obj):
        dt = datetime(2009, 2, 13, 23, 31, 30)
        expected = 1234567890
        actual = df_test_obj.convert_dt_to_api_timestring(dt)
        assert actual == expected

    def test_convert_api_timestring_to_iso8601(self, df_test_obj):
        input = 1234567890
        actual = df_test_obj.convert_api_timestring_to_iso8601(input)
        expected = "2009-02-13T23:31:30+00:00"
        assert actual == expected

    def test_replace_hardtoken_keys_two_tokens(self, df_test_obj):
        description = {
            'hardtokens': OrderedDict([
                ('hardtoken-ab-1234', {
                    'serialnumber': '1234',
                    'platform': 'ab',
                    'totp_step': '',
                }),
                ('hardtoken-cd-5678', {
                    'serialnumber': '5678',
                    'platform': 'cd',
                    'totp_step': '',
                })
            ])
        }
        df_test_obj.replace_hardtoken_keys(description, 'fake_timestamp')
        expected = OrderedDict([
            ('hardtoken_1', {
                'serialnumber': '1234',
                'platform': 'ab',
                'totp_step': ''
            }),
            ('hardtoken_2', {
                'serialnumber': '5678',
                'platform': 'cd',
                'totp_step': ''
            })
        ])
        assert expected == description

    def test_replace_hardtoken_keys_mismatch_platform(self, df_test_obj):
        description = {
            'hardtokens': {
                'hardtoken-ab-1234': {
                    'serialnumber': '1234',
                    'platform': 'yz',
                    'totp_step': '',
                },
            }
        }
        with pytest.raises(ValueError):
            df_test_obj.replace_hardtoken_keys(description, 'fake_timestamp')

    def test_replace_hardtoken_keys_mismatch_serial_number(self, df_test_obj):
        description = {
            'hardtokens': {
                'hardtoken-ab-1234': {
                    'serialnumber': '9999',
                    'platform': 'ab',
                    'totp_step': '',
                },
            }
        }
        with pytest.raises(ValueError):
            df_test_obj.replace_hardtoken_keys(description, 'fake_timestamp')

    def test_replace_hardtoken_keys_none_for_hardtokens(self, df_test_obj):
        description = {'hardtokens': None}
        df_test_obj.replace_hardtoken_keys(description, 'fake_timestamp')
        expected = {'hardtokens': None}
        assert description == expected

    def test_replace_hardtoken_keys_single_hardtoken_value_is_none(self, df_test_obj):
        description = {
            'hardtokens': {
                'hardtoken-ab-1234': None
            }
        }
        df_test_obj.replace_hardtoken_keys(description, 'fake_timestamp')
        expected = {
            'hardtoken_1': {
                'platform': 'ab',
                'serialnumber': '1234',
            }
        }
        assert description == expected

    def test_replace_hardtoken_keys_unexpected_hardtoken_format(self, df_test_obj):
        description = {
            'hardtokens': {
                'hardtoken_odd_format': None
            }
        }
        df_test_obj.replace_hardtoken_keys(description, 'fake_timestamp')
        expected = {
            'hardtokens': {
                'hardtoken_odd_format': None
            }
        }
        assert description == expected
