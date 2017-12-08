# -*- coding: utf-8 -*-
"""
Logging utilities for writing logs in a JSON format.
That allows to ingest logs into Elasticsearch cluster.
"""
from __future__ import absolute_import

import logging
import simplejson
import staticconf

from log_feeder.utils import format_dt_as_iso8601
from log_feeder.utils import get_current_time_with_utc_timezone


def current_time_in_iso_8601():
    """Returns current time in ISO 8601 format.
    """
    current_time = get_current_time_with_utc_timezone()
    return format_dt_as_iso8601(current_time)


class JsonLogger(object):

    event_data = dict()

    NAMESPACE = 'clog'
    DEFAULT_LOG_STREAM_NAME = 'tmp_batch_logfeeder'
    clog_namespace = staticconf.NamespaceGetters(NAMESPACE)
    log_stream_name = clog_namespace.get_string(
        'log_stream_name', default=DEFAULT_LOG_STREAM_NAME)

    @classmethod
    def init_logger(cls, **event_data):
        """Initializes the logger with a set of event parameters that will be
        used in the subsequent `log_event()` calls when logging a specific
        event.
        """
        cls.event_data = event_data

    @classmethod
    def log_event(cls, event_name, **event_data):
        """Logs `items` dictionary as a JSON object into the default log.
        It also adds the timestamp in ISO 8601 format in the `timestamp` field.

        :param event_name: The name of the event to log.
        :type event_name: str
        :param event_data: A dictionary containing additional parameters
                           associated with the event.
        :type event_data: dict
        """
        event_data['event'] = event_name
        event_data['timestamp'] = current_time_in_iso_8601()

        # merge class event_data into the actual event_data
        event_data.update(cls.event_data)
        json_event_data = simplejson.dumps(event_data)
        logging.info({cls.log_stream_name.value: json_event_data})
