# -*- coding: utf-8 -*-
"""
Handles S3 event notifications to retrieve a list of bucket and key pairs.

For more information about S3 event notifications see:
http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
"""
from __future__ import absolute_import

from collections import defaultdict

import boto.sqs
import simplejson
from boto.sqs.message import RawMessage

from log_feeder.profiling import measure_elapsed_time


class S3EventNotificationsHandler(object):

    def __init__(
            self, queue_name, number_messages, aws_region,
            owner_account_id=None):
        """Creates a new S3EventNotificationsHandler object and sets up
        the S3 event notifications SQS queue connection.

        :param queue_name: The name of the SQS queue containing S3 event
                           notifications.
        :type queue_name: string
        :param number_messages: The maximum number of messages to read from
                                the S3 event notifications queue.
        :type number_messages: int
        :param aws_region: The AWS region in which the S3 event notifications
                           queue is available.
        :type aws_region: string
        :param owner_account_id: The id of the account which owns the S3 event
                                 notifications queue.
        :type owner_account_id: string
        """
        self.queue_name = queue_name
        self.number_messages = number_messages
        self.aws_region = aws_region
        self.owner_account_id = owner_account_id
        self._setup_sqs_queue()

    def _setup_sqs_queue(self):
        """Sets up S3 event notifications' queue connection.
        """
        sqs_conn = boto.sqs.connect_to_region(self.aws_region)
        self.sqs_queue = sqs_conn.get_queue(
            self.queue_name, owner_acct_id=self.owner_account_id)

        if not self.sqs_queue:
            raise SqsQueueNotFoundException(self.queue_name)

        self.sqs_queue.set_message_class(RawMessage)

    @measure_elapsed_time('s3_event_notifications_received')
    def get_s3_event_notifications(self):
        """Gets S3 event notifications messages from an SQS queue.

        :returns: S3 event notifications messages.
        "rtype: list
        """
        s3_event_notifications = self.sqs_queue.get_messages(
            self.number_messages)
        return s3_event_notifications

    @measure_elapsed_time('s3_keys_retrieved')
    def get_s3_keys(self, s3_event_notifications):
        """Gets the S3 buckets and keys related to the S3 event notifications.

        :returns: A dictionary containing the S3 bucket names as the keys
                  and the S3 keys as the values.
        :rtype: dict
        """
        self.s3_keys = defaultdict(list)
        for s3_event_notification in s3_event_notifications:
            self._retrieve_s3_keys_from_event_notification(
                s3_event_notification)
        return self.s3_keys

    def _retrieve_s3_keys_from_event_notification(self, s3_event_notification):
        """Gets JSON body from the S3 event notification and invokes
        a method that retrieves S3 keys from the JSON body.
        """
        json_body = s3_event_notification.get_body()
        body = simplejson.loads(json_body)
        if 'Records' in body:
            # For boto versions that go straight reading 'Records'
            records = body['Records']
        else:
            # For boto versions that include 'Message' syntax
            json_message = body['Message']
            message = simplejson.loads(json_message)
            records = message['Records']
        self._retrieve_s3_keys_from_records(records)

    def _retrieve_s3_keys_from_records(self, records):
        """Iterates over the `records` of the S3 notification JSON body and
        retrieves the S3 bucket name and key from each record.
        """
        for record in records:
            s3_bucket = record['s3']['bucket']['name']
            s3_key = record['s3']['object']['key']
            self.s3_keys[s3_bucket].append(s3_key)

    def delete_s3_event_notifications(self, s3_event_notifications):
        """Deletes S3 event notifications messages from the SQS queue.
        """
        self.sqs_queue.delete_message_batch(s3_event_notifications)


class SqsQueueNotFoundException(Exception):

    """An exception thrown when the SQS queue cannot be found."""

    def __init__(self, queue_name):
        self.queue_name = queue_name

    def __str__(self):
        return "SQS queue {0} not found.".format(self.queue_name)
