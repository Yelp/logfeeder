#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time
from collections import namedtuple


class RateLimiter(object):

    """Limits how many calls can be made per second"""

    # Duo rate limits its API to 2 calls every 1 minute
    DEFAULT_SECONDS_PER_TIMEUNIT = 30.0
    CallRecord = namedtuple('CallRecord', ['time', 'num_calls'])

    def __init__(self, calls_per_timeunit, seconds_per_timeunit=DEFAULT_SECONDS_PER_TIMEUNIT):
        self._max_calls_per_timeunit = calls_per_timeunit
        self._call_times = []
        self._outstanding_calls = 0
        self._seconds_per_timeunit = seconds_per_timeunit

    def delay_next_calls(self, num_calls=1):
        """Adds appropriate sleep to avoid making too many calls.

        Args:
            num_calls: int the number of calls which will be made
        """
        self._cull()
        while self._outstanding_calls + num_calls > self._max_calls_per_timeunit:
            time.sleep(0.5)  # yield
            self._cull()

        self._call_times.append(self.CallRecord(time=time.time(), num_calls=num_calls))
        self._outstanding_calls += num_calls

    def _cull(self):
        """Removes calls that are more than "_seconds_per_timeunit" seconds old from the queue."""
        right_now = time.time()

        cull_from = -1
        for index in xrange(len(self._call_times)):
            if right_now - self._call_times[index].time >= self._seconds_per_timeunit:
                cull_from = index
                self._outstanding_calls -= self._call_times[index].num_calls
            else:
                break

        if cull_from > -1:
            self._call_times = self._call_times[cull_from + 1:]
