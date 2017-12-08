#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from mock import MagicMock
from mock import patch

from log_feeder.rate_limiter import RateLimiter


def test_delay_next_calls():
    rl = RateLimiter(calls_per_timeunit=1, seconds_per_timeunit=30)
    rl._cull = MagicMock()
    with patch('time.time', return_value=1111111770):
        rl.delay_next_calls(num_calls=1)
        actual = rl._call_times
        expected = [RateLimiter.CallRecord(time=1111111770, num_calls=1)]
        assert actual == expected
        actual = rl._outstanding_calls
        expected = 1
        assert actual == expected


def test__cull():
    rl = RateLimiter(calls_per_timeunit=1, seconds_per_timeunit=30)
    rl._call_times = [
        RateLimiter.CallRecord(time=1111111700, num_calls=1),
        RateLimiter.CallRecord(time=1111111770, num_calls=1),
    ]
    with patch('time.time', return_value=1111111780):
        rl._cull()
        expected = [RateLimiter.CallRecord(time=1111111770, num_calls=1)]
        actual = rl._call_times
        assert expected == actual
