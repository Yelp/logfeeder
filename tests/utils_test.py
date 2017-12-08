#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from contextlib import nested
from datetime import datetime

import dateutil.parser
import pytest
from mock import MagicMock
from mock import patch
from pytz import utc

from log_feeder.utils import datetime_to_unixtime
from log_feeder.utils import format_dt_as_iso8601
from log_feeder.utils import get_aws_connection
from log_feeder.utils import get_current_time_with_utc_timezone
from log_feeder.utils import get_sqs_queue
from log_feeder.utils import lock_and_load
from log_feeder.utils import read_yaml_file


def test_get_current_time_with_utc_timezone():
    now = get_current_time_with_utc_timezone()
    assert now.tzinfo == utc


def test_format_dt_as_iso8601_iso_ts():
    input = dateutil.parser.parse("2001-02-03T00:00:00Z")
    actual = format_dt_as_iso8601(input)
    expected = "2001-02-03T00:00:00+00:00"
    assert actual == expected


def test_format_dt_as_iso8601_pdt_ts():
    input = dateutil.parser.parse("2001-02-03T00:00:00-07:00")
    actual = format_dt_as_iso8601(input)
    expected = "2001-02-03T07:00:00+00:00"
    assert actual == expected


def test_format_dt_as_iso8601_no_timezone_1():
    # If there's no timezone info, format_dt_as_iso8601 should raise an
    # exception
    input = dateutil.parser.parse("2001-02-03 00:00:00")
    with pytest.raises(ValueError):
        format_dt_as_iso8601(input)


def test_format_dt_as_iso8601_no_timezone_2():
    # utcnow() sets no tzinfo for datetime objects
    input = datetime.utcnow()
    with pytest.raises(ValueError):
        format_dt_as_iso8601(input)


def test_datetime_to_unixtime():
    dt = datetime(2009, 2, 13, 23, 31, 30)
    expected = 1234567890
    actual = datetime_to_unixtime(dt)
    assert actual == expected


def test_lock_and_load():
    with nested(
        patch('log_feeder.utils.os.path.isdir', return_value=True),
        patch('__builtin__.open'),
        patch('log_feeder.utils.fcntl'),
        patch('log_feeder.utils.os.remove')
    ):
        fake_lf_obj = MagicMock()
        lock_and_load(fake_lf_obj)
        assert fake_lf_obj.start.call_count == 1


def test_read_yaml_file_no_keys_missing():
    yaml_content = '{k1: v1, k2: v2, k3: v3}\n'
    with patch('log_feeder.utils.read_content_from_file', return_value=yaml_content):
        actual = read_yaml_file('fake_filename', required_keys=['k1', 'k3'])
        expected = {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}
        assert actual == expected


def test_read_yaml_file_missing_key():
    yaml_content = '{k1: v1, k2: v2, k3: v3}\n'
    with patch('log_feeder.utils.read_content_from_file', return_value=yaml_content):
        with pytest.raises(KeyError):
            read_yaml_file('fake_filename', required_keys=['k1', 'k4'])


def test_get_sqs_queue():
    with patch('log_feeder.utils.get_aws_connection', return_value=MagicMock(
            get_queue=MagicMock(return_value='fake_queue_obj'))):
        with patch('log_feeder.utils.read_yaml_file', return_value={'queue_name': 'fake_queue_name'}):
            actual = get_sqs_queue('fake_aws_config_file')
            expected = 'fake_queue_obj'
            assert actual == expected


def test_get_aws_connection_no_keys():
    fake_conn_fetcher = MagicMock(return_value='fake_conn')
    with patch('log_feeder.utils.read_yaml_file', return_value={'region_name': 'fake_region_name'}):
        actual = get_aws_connection('fake_aws_config_file', fake_conn_fetcher)
        expected = 'fake_conn'
        assert actual == expected


def test_get_aws_connection_with_keys():
    fake_conn_fetcher = MagicMock(return_value='fake_conn')
    with patch('log_feeder.utils.read_yaml_file', return_value={
        'region_name': 'fake_region_name',
        'aws_access_key_id': 'abc',
        'aws_secret_access_key': '123',
    }):
        actual = get_aws_connection('fake_aws_config_file', fake_conn_fetcher)
        expected = 'fake_conn'
        assert actual == expected
