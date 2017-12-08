# -*- coding: utf-8 -*-
from __future__ import absolute_import

import gzip
from contextlib import nested
from io import BytesIO
from StringIO import StringIO

import boto
import simplejson
from boto.s3.connection import S3Connection
from boto.sts import STSConnection
from mock import ANY
from mock import MagicMock
from mock import patch

from log_feeder.s3_event_notifications import S3EventNotificationsHandler
from log_feeder.s3_feeder import S3Feeder
from log_feeder.utils import read_content_from_file
from tests.s3_event_notifications_test import read_s3_event_notifications_file
from tests.sqs_feeder_test import assert_key_values_found
from tests.sqs_feeder_test import assert_records_equal
from tests.sqs_feeder_test import SharedTestLogFeeder


def add_s3_feeder_config(s3_feeder):
    s3_feeder.s3_event_notifications_queue_name = 'cave-canem'
    s3_feeder.number_messages = 2
    s3_feeder.aws_region = 'eu-central-1'
    s3_feeder.owner_account_id = '123123'


def _run_make_queries(
        s3_feeder, test_data_dir_path, s3_event_notifications_file_name,
        log_file_name, mock_get_bucket, **expected_key_values):
    """Mocks bunch of things like S3 Event Notifications Handler and calls
    `_make_queries`.
    """
    s3_event_notifications_file_path = '{0}/{1}'.format(
        test_data_dir_path, s3_event_notifications_file_name)
    json_s3_event_notifications = read_s3_event_notifications_file(
        s3_event_notifications_file_path)
    s3_event_message_mocks = \
        _mock_get_s3_event_notifications(json_s3_event_notifications)
    s3_event_message_mocks_batches = \
        _create_s3_event_message_mocks_batches(s3_event_message_mocks)

    log_file_path = '{0}/{1}'.format(test_data_dir_path, log_file_name)
    log_file_content = read_content_from_file(log_file_path)
    gzipped_log_file = _gzip_log_file(log_file_content)
    _mock_get_bucket_contents_as_string(mock_get_bucket, gzipped_log_file)
    expected_records = s3_feeder.transform_records_from_log_file(
        'cloudtrail-logs/cloudtrail/someKey',
        log_file_content)

    with nested(
        patch.object(
            S3EventNotificationsHandler, '__init__', autospec=True,
            return_value=None),
        patch.object(
            S3EventNotificationsHandler, 'get_s3_event_notifications',
            autospec=True, side_effect=s3_event_message_mocks_batches),
        patch.object(
            S3EventNotificationsHandler, 'delete_s3_event_notifications',
            autospec=True),
    ) as (patched_s3_event_notifications_handler,
            patched_get_s3_event_notifications,
            patched_delete_s3_event_notifications):
        records_generator = s3_feeder._make_queries_wrapper(
            'this', 'is', 'not', 'necessary')

        assert_key_values_found(expected_records, **expected_key_values)
        assert_records_equal(expected_records, list(records_generator))

        patched_s3_event_notifications_handler.assert_called_once_with(
            ANY, 'cave-canem', 2, 'eu-central-1', '123123')
        assert patched_get_s3_event_notifications.called

    _assert_get_s3_event_notifications_called(
        patched_get_s3_event_notifications, s3_event_message_mocks,
        patched_delete_s3_event_notifications)
    return len(json_s3_event_notifications)


def _mock_get_s3_event_notifications(json_s3_event_notifications):
    """Mocks responses to the get_s3_event_notifications method from
    S3EventNotificationsHandler.
    """
    s3_event_message_mocks = [
        MagicMock(**{'get_body.return_value': json_s3_event_notification})
        for json_s3_event_notification in json_s3_event_notifications]
    return s3_event_message_mocks


def _create_s3_event_message_mocks_batches(s3_event_message_mocks):
    """Turns s3_event_message_mocks list into a list of two-element lists
    so that get_messages will return 2 messages in the consecutive calls.
    As _make_query is a generator working until get_messages returns any
    messages, the mock returns batches of 2 messages in subsequent calls.
    """
    number_message_batches = _calculate_number_message_batches(
        s3_event_message_mocks)
    s3_event_message_mocks_batches = [
        s3_event_message_mocks[2 * i:2 * (i + 1)]
        for i in range(number_message_batches)]
    return s3_event_message_mocks_batches


def _calculate_number_message_batches(s3_event_message_mocks):
    """Calculates the number of message batches, i.e. the number of times
    get_messages will return some messages.
    """
    number_message_batches = (len(s3_event_message_mocks) + 1) / 2
    return number_message_batches


def _gzip_log_file(log_file_content):
    """Produces gzipped version of a log file content.
    """
    gzipped_logs = StringIO()
    with gzip.GzipFile(fileobj=gzipped_logs, mode='w') as gzip_file:
        gzip_file.writelines(log_file_content)
    return gzipped_logs


def _mock_get_bucket_contents_as_string(mock_get_bucket, gzipped_log_file):
    """Mocks get_bucket get_key content_as_string value to be the gzipped log
    file content.
    """
    mock_get_key = mock_get_bucket.get_key
    mock_get_contents_as_string = \
        mock_get_key.return_value.get_contents_as_string
    mock_get_contents_as_string.return_value = \
        gzipped_log_file.getvalue()


def _assert_get_s3_event_notifications_called(
        mock_get_s3_event_notifications, s3_event_message_mocks,
        mock_delete_s3_event_notifications):
    """Asserts get_s3_event_notifications mock was called with the
    number_message parameter value and that the number of calls is equal
    to the number of message batches.
    Also ensures that each message mock was called the appropriate number
    of times and delete_message was called as well.
    """
    number_message_batches = _calculate_number_message_batches(
        s3_event_message_mocks)
    # assert get_messages called one more time than number of message
    # batches
    assert number_message_batches + 1 == \
        mock_get_s3_event_notifications.call_count
    for s3_event_message_mock in s3_event_message_mocks:
        assert s3_event_message_mock.get_body.called
    assert number_message_batches == \
        mock_delete_s3_event_notifications.call_count


def run_make_queries(
        s3_feeder, test_data_dir_path, s3_event_notifications_file_name,
        log_file_name, expected_bucket_name, **expected_key_values):
    _run_make_queries_no_role_arn(
        s3_feeder, test_data_dir_path, s3_event_notifications_file_name,
        log_file_name, expected_bucket_name, **expected_key_values)
    _run_make_queries_with_role_arn(
        s3_feeder, test_data_dir_path, s3_event_notifications_file_name,
        log_file_name, expected_bucket_name, **expected_key_values)


def _run_make_queries_no_role_arn(
        s3_feeder, test_data_dir_path, s3_event_notifications_file_name,
        log_file_name, expected_bucket_name, **expected_key_values):
    s3_feeder.role_arn = None
    boto.connect_s3 = MagicMock()
    mock_get_bucket = boto.connect_s3.return_value.get_bucket

    expected_get_bucket_call_count = _run_make_queries(
        s3_feeder, test_data_dir_path, s3_event_notifications_file_name,
        log_file_name, mock_get_bucket.return_value, **expected_key_values)

    assert boto.connect_s3.called
    mock_get_bucket.assert_called_with(expected_bucket_name, validate=False)
    assert expected_get_bucket_call_count == mock_get_bucket.call_count


def _run_make_queries_with_role_arn(
        s3_feeder, test_data_dir_path, s3_event_notifications_file_name,
        log_file_name, expected_bucket_name, **expected_key_values):
    s3_feeder.role_arn = 'arn:aws:iam::987:role/s3-bucket-all-read'
    mock_assume_role = MagicMock()
    mock_assume_role.credentials.access_key = 'dummy-access-key'
    mock_assume_role.credentials.secret_key = 'very-secret-key'
    mock_assume_role.credentials.session_token = 'some token'

    mock_get_bucket = MagicMock()

    with nested(
        patch.object(
            STSConnection, '__init__', autospec=True, return_value=None),
        patch.object(
            STSConnection, 'assume_role', autospec=True,
            return_value=mock_assume_role),
        patch.object(
            S3Connection, '__init__', autospec=True, return_value=None),
        patch.object(
            S3Connection, 'get_bucket', autospec=True,
            return_value=mock_get_bucket)
    ) as (__, patched_assume_role, patched_s3_connection_init,
            patched_get_bucket):
        expected_get_bucket_call_count = _run_make_queries(
            s3_feeder, test_data_dir_path, s3_event_notifications_file_name,
            log_file_name, mock_get_bucket, **expected_key_values)

    patched_assume_role.assert_called_with(
        ANY,
        role_arn='arn:aws:iam::987:role/s3-bucket-all-read',
        role_session_name='AssumeRoleS3BucketRead')
    patched_s3_connection_init.assert_called_with(
        ANY,
        aws_access_key_id='dummy-access-key',
        aws_secret_access_key='very-secret-key',
        security_token='some token')
    patched_get_bucket.assert_called_with(
        ANY, expected_bucket_name, validate=False)
    assert expected_get_bucket_call_count == patched_get_bucket.call_count


class SharedTestS3Feeder(SharedTestLogFeeder):

    def sub_api_run_tester(
            self, s3_feeder, make_queries_output,
            expected_final_records_dir_path, expected_final_records_file_name):
        s3_feeder.domain = 'logfeeder.example.com'
        expected_final_records_file_path = '{0}/{1}'.format(
            expected_final_records_dir_path, expected_final_records_file_name)
        with open(expected_final_records_file_path) as fp:
            expected_final_records = simplejson.load(fp)

        super(SharedTestS3Feeder, self).sub_api_run_tester(
            s3_feeder, 'dummy sub-API', 'yellow', make_queries_output,
            expected_final_records)


class TestS3Feeder(object):

    def test_parse_config(self):
        main_config_file = BytesIO(
            'logfeeder:\n'
            '  aws_config_filepath: /some/dummy/file\n'
            '  domain: s3-feeder-testing.example.com\n'
            's3feeder:\n'
            '  file: /another/dummy/path\n'
        )
        s3_feeder_config_file = BytesIO(
            'api_creds_filepath: api_creds_dummy_filepath\n'
            's3_event_notifications_queue_name: s3-bucket-events\n'
            'aws_region: aws-from-down-under\n'
            'owner_account_id: 456321\n'
            'role_arn: arn:aws:iam::123:role/s3-bucket-all-read\n'
        )
        with patch('__builtin__.open', side_effect=[main_config_file,
                                                    s3_feeder_config_file]):
            options = MagicMock()
            options.config_path = 'path/config.yaml'
            options.instance_name = 'Test' 
            options.stateless = True
            options.hidden_tag = False 
            options.no_output = True


            s3_feeder = S3Feeder(options=options)
            # this is necessary so that SqsOutput knows which section to look
            # for in main_config_file
            s3_feeder.APP_NAME = 's3feeder'
            s3_feeder.parse_config('this_is_some_config_file')

        assert 's3-bucket-events' == \
            s3_feeder.s3_event_notifications_queue_name
        assert 'aws-from-down-under' == s3_feeder.aws_region
        assert '456321' == s3_feeder.owner_account_id
        assert 'arn:aws:iam::123:role/s3-bucket-all-read' == s3_feeder.role_arn
