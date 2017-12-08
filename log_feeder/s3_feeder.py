# -*- coding: utf-8 -*-
"""
S3 Feeder is based on the concept of S3 bucket event notifications:
http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html

For large volume logs in S3 (CloudTrail, OpenDNS) listing the bucket and then
fetching the deltas may take significantly long time. S3 has very low
performance in attempting to read for a bucket with a high number of writes.
The S3 reader would then spend most of the time listing the bucket contents
rather than reading the keys.

Thanks to S3 event notifications, S3 Feeder can receive messages from SQS
queue which will contain the bucket updates, listing the newly created keys.
S3 Feeder reads the messages and then fetches the key content, which in most
cases (CloudTrail, OpenDNS) is gzipped. After unzipping it, the content is
converted to the list of records that can be further processed by SQS Feeder.
"""
from __future__ import absolute_import

import gzip
import os
import subprocess
import time
import urllib
from cStringIO import StringIO

import boto
import boto.sqs
import dateutil.parser
import staticconf
from boto.s3.connection import S3Connection
from boto.sts import STSConnection

from log_feeder.base_feeder import BaseFeeder
from log_feeder.json_logging import JsonLogger
from log_feeder.s3_event_notifications import S3EventNotificationsHandler
from log_feeder.utils import format_dt_as_iso8601


class S3Feeder(BaseFeeder):

    """
    Abstract class that contains all of the common parts for the S3 Feeder,
    The concrete classes in principle need to implement only the
    `extract_records_from_log_file` method.
    """

    MAXIMUM_MESSAGE_SIZE = 256 * 1024  # 262144 bytes

    def __init__(self, **kwargs):
        super(S3Feeder, self).__init__(**kwargs)
        self._sqs_msg_chunk_size = self.MAXIMUM_MESSAGE_SIZE
        self.do_write_latest_event_time = False

    def parse_config(self, config_file_path):
        super(S3Feeder, self).parse_config(config_file_path)
        self.s3_event_notifications_queue_name = staticconf.read(
            's3_event_notifications_queue_name')
        self.number_messages = staticconf.read('number_messages', default=1)
        self.aws_region = staticconf.read('aws_region', default=None)
        self.owner_account_id = staticconf.read_string('owner_account_id')
        self.role_arn = staticconf.read('role_arn', default=None)

    def read_api_creds(self, api_creds_filepath):
        # there are no credentials for S3-based logs
        pass

    def _make_queries_wrapper(self, *args):
        return self._make_queries(*args)

    def _make_queries(
            self, start_times_dict, end_time, api_info_dict, sub_api_name):
        """Retrieves S3 event notifications from an SQS queue and fetches
        the log files from the S3 keys mentioned in these notifications.
        It converts the log files into the lists of log records, which are
        yielded and processed by the parent class.
        The S3 event notifications are deleted from the SQS queue after the
        logs are yielded. As there is `while True` loop the deletion will
        always happen before the method terminates even after the last yield.
        """
        s3_event_notifications_handler = S3EventNotificationsHandler(
            self.s3_event_notifications_queue_name, self.number_messages,
            self.aws_region, self.owner_account_id)
        self._create_s3_connection()
        while True:
            before_get_s3_event_notifications_time = time.time()
            s3_event_notifications = \
                s3_event_notifications_handler.get_s3_event_notifications()
            if s3_event_notifications:
                s3_keys = s3_event_notifications_handler.get_s3_keys(
                    s3_event_notifications)

                log_files = self._get_log_files_from_s3_keys(s3_keys)
                for records in self._extract_records_from_log_files(log_files):
                    yield records

                s3_event_notifications_handler.delete_s3_event_notifications(
                    s3_event_notifications)
                after_delete_message_batch_time = time.time()

                total_time = after_delete_message_batch_time - \
                    before_get_s3_event_notifications_time
                s3_event_notifications_length = len(s3_event_notifications)
                average_time_per_message = \
                    total_time / s3_event_notifications_length
                JsonLogger.log_event(
                    'message_batch_processed', elapsed_time=total_time,
                    messages_length=s3_event_notifications_length,
                    average_processing_time=average_time_per_message)
            else:
                return

    def _get_log_files_from_s3_keys(self, s3_keys):
        """Fetches the keys' content from S3 and transforms them into a list
        of log files.
        """
        for bucket, keys in s3_keys.items():
            for key in keys:
                yield os.path.join(bucket, key), self._read_s3_key_content(bucket, key)

    def get_file(self, bucket, key):
        if not self.s3_connection:
            self._create_s3_connection()
        return self._read_s3_key_content(bucket, key)

    def _create_s3_connection(self):
        """Creates S3 connection either assuming role or not,
        based on the `role_arn` parameter in configuration.
        """
        if self.role_arn:
            self._create_s3_connection_assuming_role()
        else:
            self.s3_connection = boto.connect_s3()

    def _create_s3_connection_assuming_role(self):
        """Creates S3 connection by assuming the role necessary to read from
        the S3 bucket that contains the logs.
        """
        JsonLogger.log_event('assuming_role', assumed_role_arn=self.role_arn)
        sts_connection = STSConnection()
        assumed_role = sts_connection.assume_role(
            role_arn=self.role_arn,
            role_session_name="AssumeRoleS3BucketRead"
        )
        self.s3_connection = S3Connection(
            aws_access_key_id=assumed_role.credentials.access_key,
            aws_secret_access_key=assumed_role.credentials.secret_key,
            security_token=assumed_role.credentials.session_token
        )

    def _read_s3_key_content(self, s3_bucket, s3_key):
        """Retrieves the content of the S3 key and unzips the content.
        """
        JsonLogger.log_event(
            'reading_s3_key_content', s3_bucket=s3_bucket, s3_key=s3_key)
        bucket = self.s3_connection.get_bucket(s3_bucket, validate=False)
        key = bucket.get_key(urllib.unquote(s3_key))
        try:
            p = subprocess.Popen(['zcat'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            content, __ = p.communicate(key.get_contents_as_string())
            assert p.returncode == 0
        except (AssertionError, OSError) as e:
            JsonLogger.log_event('gzip_decode_error', error=e)
            gzip_contents = StringIO(key.get_contents_as_string())
            content = gzip.GzipFile(fileobj=gzip_contents).read()
        return content

    def _extract_records_from_log_files(self, log_files):
        """Extracts the records (the actual list of log entries) from each log
        file.
        """
        for s3_path, log_file_content in log_files:
            yield self.transform_records_from_log_file(s3_path, log_file_content)

    def transform_records_from_log_file(self, s3_path, log_file_content):
        """This is an abstract method that should be implemented in the
        concrete class. It should transform the log records from the
        `log_file_content` and return them as a list. The `s3_path` is also
        provided in case the name of the key is needed to identify its content.

        The expected type of each records object is `dict`.

        Returns:
            A list of dictionaries. Each dictionary should represent a log
            record.
        """
        raise NotImplementedError

    def convert_dt_to_api_timestring(self, datetime_obj):
        return format_dt_as_iso8601(datetime_obj)

    def convert_api_timestring_to_iso8601(self, api_timestring):
        return format_dt_as_iso8601(dateutil.parser.parse(api_timestring))
