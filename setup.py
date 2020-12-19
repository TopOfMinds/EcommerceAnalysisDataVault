#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name = "dwutility",
    version = "0.1",
    url = "https://github.com/TopOfMinds/EcommerceAnalysisDataVault",
    description="DW utility",
    entry_points='''
        [console_scripts]
        dwutility=dwutility.cli:cli
    ''',
    packages = find_packages(),
    install_requires=[
        'Click',
        'Jinja2',
        'colorama',
        'snowflake-connector-python',
        'oauth2client'
    ],
    setup_requires=['setuptools_git'],
    include_package_data=True,
)
