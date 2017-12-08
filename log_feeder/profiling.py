# -*- coding: utf-8 -*-
"""
Provides a decorator that can be applied to any method to measure the time it
has taken to execute it.
"""
from __future__ import absolute_import

import time
import logging


def measure_elapsed_time(event_name):
    """Decorator to log the time that elapsed when the function was executing.

    :param event_name: The event name that will be used when logging the
                       elapsed time.
    :type event_name: str
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()

            result = func(*args, **kwargs)

            end_time = time.time()
            elapsed_time = end_time - start_time
            logging.info(event_name, elapsed_time)
            return result
        return wrapper
    return decorator
