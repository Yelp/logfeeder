# -*- coding: utf-8 -*-
from __future__ import absolute_import

import boto.sqs
import pytest
import simplejson
from boto.sqs.message import RawMessage
from mock import MagicMock

from log_feeder.s3_event_notifications import S3EventNotificationsHandler
from log_feeder.s3_event_notifications import SqsQueueNotFoundException


TEST_DATA_DIR_PATH = 'tests/data/cloudtrail'


def read_s3_event_notifications_file(s3_event_notifications_file_path):
    """Reads S3 event notifications and serializes them into JSON format so
    that they can be used as the message bodies in mocked get_messages.
    """
    with open(s3_event_notifications_file_path) as fp:
        s3_event_notifications = simplejson.load(fp)
        json_s3_event_notifications = [
            simplejson.dumps(s3_event_notification)
            for s3_event_notification in s3_event_notifications
        ]
    return json_s3_event_notifications


def create_s3_event_notification_message_mocks(
        s3_event_notifications_file_name):
    """Creates SQS queue message mocks that will return the JSON content of
    `s3_event_notifications_file_path` JSON file as the body of the message.
    """
    s3_event_notifications_file_path = '{0}/{1}'.format(
        TEST_DATA_DIR_PATH, s3_event_notifications_file_name)
    json_s3_event_notifications = read_s3_event_notifications_file(
        s3_event_notifications_file_path)
    s3_event_notification_message_mocks = [
        MagicMock(**{'get_body.return_value': json_s3_event_notification})
        for json_s3_event_notification in json_s3_event_notifications]
    return s3_event_notification_message_mocks


class TestS3EventNotificationsHandler(object):

    @pytest.fixture
    def mock_connect_to_region(self):
        connect_to_region_mock = MagicMock()
        boto.sqs.connect_to_region = connect_to_region_mock
        return connect_to_region_mock

    def assert_get_queue_called_with(
            self, mock_get_queue, queue_name, owner_account_id):
        mock_get_queue.assert_called_with(
            queue_name, owner_acct_id=owner_account_id)
        mock_sqs_queue = mock_get_queue.return_value
        mock_sqs_queue.set_message_class.assert_called_with(RawMessage)

    def test_init(self, mock_connect_to_region):
        S3EventNotificationsHandler('abracadabra', 10, 'us-west-1', '123456')

        mock_connect_to_region.assert_called_with('us-west-1')
        mock_get_queue = mock_connect_to_region.return_value.get_queue
        self.assert_get_queue_called_with(
            mock_get_queue, 'abracadabra', '123456')

    def test_init_no_sqs_queue_found(self, mock_connect_to_region):
        mock_get_queue = mock_connect_to_region.return_value.get_queue
        mock_get_queue.return_value = None

        with pytest.raises(SqsQueueNotFoundException) as exception_info:
            S3EventNotificationsHandler(
                'open-sesame', 1, 'eu-east-4', '111222')
        assert 'open-sesame' in str(exception_info.value)

    @pytest.fixture
    def s3_event_notifications_handler(self, mock_connect_to_region):
        handler = S3EventNotificationsHandler(
            'sator-square', 5, 'eu-west-1', '999333')
        return handler

    def test_get_s3_event_notifications(self, s3_event_notifications_handler):
        s3_event_notification_message_mocks = \
            create_s3_event_notification_message_mocks(
                's3_event_single_notification.json')
        mock_sqs_queue = MagicMock()
        mock_sqs_queue.get_messages.return_value = \
            s3_event_notification_message_mocks
        s3_event_notifications_handler.sqs_queue = mock_sqs_queue

        s3_event_notifications = \
            s3_event_notifications_handler.get_s3_event_notifications()

        assert s3_event_notifications == \
            mock_sqs_queue.get_messages.return_value

    def test_get_s3_keys_empty_s3_event_notification_message(
            self, s3_event_notifications_handler):
        empty_s3_event_notification_message = \
            create_s3_event_notification_message_mocks(
                'empty_s3_event_notification_message.json')

        with pytest.raises(KeyError) as exception_info:
            s3_event_notifications_handler.get_s3_keys(
                empty_s3_event_notification_message)

        assert 'Message' in str(exception_info.value)

    def assert_s3_keys_contain(self, bucket_name, expected_keys, s3_keys):
        """Asserts that `s3_keys` dictionary contains `bucket_name` key and
        that the value stored under this key is the same list of S3 keys
        as expected in `expected_keys`.
        """
        assert bucket_name in s3_keys
        actual_keys = s3_keys[bucket_name]
        assert len(expected_keys) == len(actual_keys)
        for key in expected_keys:
            assert key in actual_keys

    def test_get_s3_keys(self, s3_event_notifications_handler):
        s3_event_notification_messages = \
            create_s3_event_notification_message_mocks(
                's3_event_notification_messages.json')

        s3_keys = s3_event_notifications_handler.get_s3_keys(
            s3_event_notification_messages)

        assert 2 == len(s3_keys)
        cloudtrail_logs_a_keys = [
            'cloudtrail/bucket_logs_storage_10-00-00-01/234345635624/the_action_logs/eu-east-4/2015/06/18/'
            '234345635624_the_action_logs_eu-east-4_20150618T1805Z_QmqybQid0fHXLewO'
            '.json.gz',
            'cloudtrail/bucket_logs_storage_10-00-00-01/234345635624/the_action_logs/eu-east-4/2015/06/18/'
            '234345635624_the_action_logs_eu-east-4_20150618T1655Z_zDWKeAkBgGJi0ELl'
            '.json.gz',
            'cloudtrail/bucket_logs_storage_10-00-00-01/234345635624/the_action_logs/eu-east-4/2015/06/19/'
            '234345635624_the_action_logs_eu-east-4_20150619T1155Z_10fOUn9G84fWF29e'
            '.json.gz',
            'cloudtrail/bucket_logs_storage_10-00-00-01/234345635624/the_action_logs/eu-east-4/2015/06/18/'
            '234345635624_the_action_logs_eu-east-4_20150618T1835Z_2sQlwOkAWsdaCeDu'
            '.json.gz',
        ]
        self.assert_s3_keys_contain(
            'cloudtrail-logs-a', cloudtrail_logs_a_keys, s3_keys)
        cloudtrail_logs_b_keys = [
            'cloudtrail/bucket_logs_storage_10-00-00-01/234345635624/the_action_logs/eu-east-4/2015/06/19/'
            '234345635624_the_action_logs_eu-east-4_20150619T1155Z_2LKYBVsKv0MW3csl'
            '.json.gz',
            'cloudtrail/bucket_logs_storage_10-00-00-01/234345635624/the_action_logs/eu-east-4/2015/06/18/'
            '234345635624_the_action_logs_eu-east-4_20150618T1835Z_k3qRqzN2jBaNj6VM'
            '.json.gz',

        ]
        self.assert_s3_keys_contain(
            'cloudtrail-logs-b', cloudtrail_logs_b_keys, s3_keys)

    def test_delete_s3_event_notifications(
            self, s3_event_notifications_handler):
        s3_event_notification_messages = \
            create_s3_event_notification_message_mocks(
                's3_event_notifications.json')
        mock_sqs_queue = MagicMock()
        s3_event_notifications_handler.sqs_queue = mock_sqs_queue

        s3_event_notifications_handler.delete_s3_event_notifications(
            s3_event_notification_messages)

        mock_sqs_queue.delete_message_batch.assert_called_once_with(
            s3_event_notification_messages)

    def test_newer_boto_s3_event_notifications(
            self, s3_event_notifications_handler):
        s3_event_notification_messages = \
            create_s3_event_notification_message_mocks(
                's3_boto_no_message_wrapper_event_notifications.json')
        mock_sqs_queue = MagicMock()
        s3_event_notifications_handler.sqs_queue = mock_sqs_queue

        s3_event_notifications_handler.delete_s3_event_notifications(
            s3_event_notification_messages)

        mock_sqs_queue.delete_message_batch.assert_called_once_with(
            s3_event_notification_messages)
