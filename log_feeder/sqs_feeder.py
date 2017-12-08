# -*- coding: utf-8 -*-
'''
Reads log data from an API and feeds it to an AWS SQS Queue.
'''
from __future__ import absolute_import

import threading

import simplejson

from log_feeder.base_feeder import BaseFeeder
from log_feeder.json_logging import JsonLogger
from log_feeder.utils import get_sqs_queue

SIXTY_FOUR_KILOBYTES = 64 * 1024
TWO_HUNDRED_FIFTY_SIX_KILOBYTES = 256 * 1024
MAX_ATTEMPTS = 3


class SqsOutput(BaseFeeder):
    def __init__(self, **kwargs):
        super(SqsOutput, self).__init__(**kwargs)
        self._sqs_msg_chunk_size = SIXTY_FOUR_KILOBYTES
        self._sqs_msg_batch_size = TWO_HUNDRED_FIFTY_SIX_KILOBYTES

    def send_records(self, records, sub_api_name):
        """Sends records with API info to the SQS queue

        Args:
            records (list of dicts): records from the API stored as a list of dicts
            sub_api_name (string): name of the sub_api
        """
        records = (simplejson.dumps(r) for r in records)
        num_threads = 3
        threads = []
        lock = threading.Lock()
        for _ in range(num_threads):
            queue = get_sqs_queue(self.aws_config_filepath)
            thread = threading.Thread(target=self._send_sqs_messages, args=(queue, records, lock))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        self.log.info("{0} SQS message(s) sent from the {1} {2} API for the {3} account".format(
            self.num_records_processed, self.APP_NAME, sub_api_name, self.domain))
        JsonLogger.log_event(
            'messages_sent_to_sqs', sub_api_name=sub_api_name,
            messages_number=self.num_records_processed)

    def _send_sqs_messages(self, sqs_queue, records, lock):
        """Sends SQS messages containing JSON records to an SQS queue

        Args:
            sqs_queue: an SQS queue object
            records: a list of JSON objects containing API data for SQS
        """
        batch = []
        batch_size = 0
        lock.acquire()
        try:
            for i, rec in enumerate(records):
                lock.release()
                if len(rec) > self._sqs_msg_chunk_size:
                    rec_dict = simplejson.loads(rec)
                    JsonLogger.log_event(
                        'message_truncated',
                        truncated_size=self._sqs_msg_chunk_size,
                        actual_size=len(rec),
                        record_event_time=rec_dict['event_time'])
                    rec = rec[:self._sqs_msg_chunk_size]
                batch_size += len(rec)
                # If the queue is full, or adding the next item will exceed its max size,
                # flush the queue and start a new one
                if batch_size > self._sqs_msg_batch_size or len(batch) == 10:
                    sqs_queue.write_batch(batch)
                    batch = [[i, rec, 0]]
                    batch_size = len(rec)
                # Otherwise, add the next item
                else:
                    batch.append([i, rec, 0])
                lock.acquire()
        finally:
            try:
                lock.release()
            except:
                pass

        # Write anything left over (i.e., if the last batch did not reach a size of 10)
        if len(batch) > 0:
            sqs_queue.write_batch(batch)
