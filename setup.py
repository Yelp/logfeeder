#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from setuptools import find_packages
from setuptools import setup

setup(
    name="LogFeeder",
    version="0.1",
    description="Reads log data from an API and sends it to the configured output plugins.",
    author="Yelp",
    setup_requires="setuptools",
    license="Copyright Yelp 2017, All Rights Reserved",
    packages=find_packages(exclude=["tests"]),
)
