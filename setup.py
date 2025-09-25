#!/usr/bin/env python

from setuptools import setup

install_requires = open("requirements.txt").read().strip().split("\n")

setup(
    name="target-bigquery",
    version="1.0.0",
    description="Singer.io target for writing data to Google BigQuery",
    author="hotglue",
    url="https://github.com/hotgluexyz/target-bigquery",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    packages=["target_bigquery"],
    install_requires=install_requires,
    extras_require={},
    entry_points="""
        [console_scripts]
        target-bigquery=target_bigquery:main
      """,
)
