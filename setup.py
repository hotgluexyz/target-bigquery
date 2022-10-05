#!/usr/bin/env python

from setuptools import setup

install_requires = open("requirements.txt").read().strip().split("\n")
dev_requires = open("dev-requirements.txt").read().strip().split("\n")

extras = {
    "dev": dev_requires
}

setup(
    name="target-bigquery",
    version="0.11.5",
    description="Singer.io target for writing data to Google BigQuery",
    author="Adswerve",
    url="https://github.com/adswerve/target-bigquery",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    packages=["target_bigquery"],
    install_requires=install_requires,
    extras_require=extras,
    entry_points="""
        [console_scripts]
        target-bigquery=target_bigquery:main
      """
)
