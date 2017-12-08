#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
EsOutput is a superclass intended to be subclassed.

Reads log data from an API and uploads it to Elasticsearch.
"""
from __future__ import absolute_import

import hashlib
import itertools
import logging
import time
from datetime import datetime

import dateutil.parser
import dateutil.tz
import simplejson
from elasticsearch.client import Elasticsearch
from elasticsearch.exceptions import ElasticsearchException


from log_feeder.base_feeder import BaseFeeder



def parse_es_options(option_parser):
    option_parser.add_option('--index', default='', help='The index pattern to use when uploading to Elasticsearch. '
                                                     'It will be strftimed to generate the specific index name. '
                                                     'Default is %default.')
    option_parser.add_option('--chunksize', default=10000, type=int, help='The size of each chunk to send to '
                         'Elasticsearch. Default is %default.')
    option_parser.add_option(
        '--hostname',
        default=['es001-eu-09-production'],
        action='append',
        help='Elasticsearch cluster hostname. Default is %default.'
    )
    option_parser.add_option('--port', default=14900, type=int, help='Elasticsearch cluster port. Default is %default')
    option_parser.add_option('--in_order', default=False, action='store_true', help='Assume records are always in '
                         'order of timestamp. Default is %default')
    return option_parser


def total_seconds(td):
    # for python2.6 compatability
    # see https://docs.python.org/2/library/datetime.html#datetime.timedelta.total_seconds
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6


class EsOutput(BaseFeeder):
    """ Like the SqsOutput, but sends records to Elasticsearch instead. """

    def __init__(self, **kwargs):
        super(EsOutput, self).__init__(**kwargs)
        es_options = kwargs['es_options']
        self.es_hostnames = es_options.hostnames
        self.es_port = es_options.port
        self.chunksize = es_options.chunksize
        self.in_order = es_options.in_order
        self.index = es_options.index

    def apply_common_filters(self, records):
        """ Apply common Elasticsearch filters. """
        # Ingestion time and timestamp
        ingestion = datetime.utcnow().isoformat() + 'Z'
        utcnow = datetime.utcnow().replace(tzinfo=dateutil.tz.tzutc())
        for record in records:
            record['@timestamp'] = record['event_time']
            record['@ingestionTime'] = ingestion
            try:
                record['@time_delta_seconds'] = total_seconds(utcnow - dateutil.parser.parse(record['event_time']))
            except TypeError:
                # Can't compare tz unaware with tz aware, leave this blank
                pass
            record_data = record.get('{}_data'.format(self.APP_NAME), {})

            # Compute a fingerprint, comprised of timestamp + the _data
            if '_id' not in record_data:
                fingerprint = hashlib.sha1(simplejson.dumps(record_data) + record['@timestamp']).hexdigest()
                record_data['_id'] = fingerprint

    def apply_specific_filters(self, records):
        """ Apply any specific filters to records. To be implemented by sub feeders. """
        pass

    def send_records(self, all_records, sub_api_name):
        """ Sends records with API info to Elasticsearch

        Args:
            records (list of dicts): records from the API stored as a list of dicts
            sub_api_name (string): name of the sub_api
        """
        es_endpoints = {es: Elasticsearch(host=es, port=self.es_port, timeout=90) for es in self.es_hostnames}
        chunksize = self.chunksize
        idx = self.index
        if type(all_records) == list:
            all_records = iter(all_records)
        while True:
            latest_ts = datetime.min.replace(tzinfo=dateutil.tz.tzutc())
            records = list(itertools.islice(all_records, chunksize))
            if not records:
                break
            # Map index names to records by timestamp
            index_to_records = {}
            start_idx = dateutil.parser.parse(records[0]['event_time']).strftime(idx)
            end_idx = dateutil.parser.parse(records[-1]['event_time']).strftime(idx)
            if self.in_order and start_idx == end_idx:
                index_to_records[start_idx] = records
            else:
                # Map records to indices
                for record in records:
                    ts = dateutil.parser.parse(record['event_time'])
                    latest_ts = max(latest_ts, ts)
                    index_to_records.setdefault(ts.strftime(idx), []).append(record)

            _type = self.APP_NAME
            for index, chunk in index_to_records.items():
                self.apply_common_filters(chunk)
                self.apply_specific_filters(chunk)
                bulk = []
                for doc in chunk:
                    doc_source = doc.get('{}_data'.format(self.APP_NAME), {})
                    _id = doc_source.get('_id')
                    if _id:
                        bulk.append(simplejson.dumps({'create': {'_type': _type, '_index': index, '_id': _id}}))
                        doc_source.pop('_id')
                    else:
                        bulk.append(simplejson.dumps({'create': {'_type': _type, '_index': index}}))
                    bulk.append(simplejson.dumps(doc))
                bulk = '\n'.join(bulk) + '\n'
                for hostname, es in es_endpoints.iteritems():
                    for retries in range(5):
                        try:
                            es.bulk(body=bulk, index=index)
                            break
                        except ElasticsearchException as e:
                            self.log.exception('Attempt {0} for sending {1} to Elasticsearch failed.'.format(
                                retries, self.APP_NAME))
                            time.sleep(2 ** retries)
                    else:
                        raise ElasticsearchException('Upload from the {0} {1} API to {2} failed. '
                                                     'Original exception: {3}'.format(
                                                         self.APP_NAME, sub_api_name, hostname, e))
            latest_ts = latest_ts.isoformat()
            self.mark_as_completed(latest_ts, sub_api_name)
            self.log.warn("{0} ES document(s) sent from the {1} {2} API for the {3} account".format(
                len(records), self.APP_NAME, sub_api_name, self.domain))
            logging.info(
                'messages_sent_to_es', sub_api_name,
                len(records))
