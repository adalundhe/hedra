import os
from setuptools import (
    setup,
    find_packages
)

current_directory = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(current_directory, 'README.md'), "r") as readme:
    package_description = readme.read()

version_string = ""
with open (os.path.join(current_directory, ".version"), 'r') as version_file:
    version_string = version_file.read()

setup(
    name="hedra",
    version=version_string,
    description="Performance testing at scale.",
    long_description=package_description,
    long_description_content_type="text/markdown",
    author="Sean Corbett",
    author_email="sean.corbett@umconnect.edu",
    url="https://github.com/scorbettUM/hedra",
    packages=find_packages(),
    keywords=[
        'pypi', 
        'cicd', 
        'python',
        'performance',
        'testing',
        'dag',
        'graph',
        'workflow'
    ],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    install_requires=[
        'aiologger',
        'yaspin',
        'attr',
        'networkx',
        'aiodns',
        'h2',
        'click',
        'psutil',
        'fastapi',
        'dill',
        'scipy',
        'art',
        'scikit-learn',
        'uvloop',
        'tdigest',
        'GitPython'
    ],
    entry_points = {
        'console_scripts': [
            'hedra=hedra.cli:run',
            'hedra-server=hedra.run_uwsgi:run_uwsgi'
        ],
    },
    extras_require = {
        'all': [
            'grpcio',
            'grpcio-tools',
            'gql',
            'playwright',
            'azure-cosmos',
            'libhoney',
            'influxdb_client',
            'newrelic',
            'aio_statsd',
            'prometheus-client',
            'prometheus-api-client',
            'cassandra-driver',
            'datadog',
            'motor',
            'redis',
            'aioredis',
            'aiomysql',
            'psycopg2-binary',
            'aiopg',
            'sqlalchemy',
            'boto3',
            'snowflake-connector-python',
            'google-cloud-bigquery',
            'google-cloud-bigtable',
            'google-cloud-storage',
        ],
        'all-engines': [
            'grpcio',
            'grpcio-tools',
            'gql',
            'playwright'

        ],
        'all-reporters': [
            'azure-cosmos',
            'libhoney',
            'influxdb_client',
            'newrelic',
            'aio_statsd',
            'prometheus-client',
            'prometheus-api-client',
            'cassandra-driver',
            'datadog',
            'motor',
            'redis',
            'aioredis',
            'aiomysql',
            'psycopg2-binary',
            'aiopg',
            'sqlalchemy',
            'boto3',
            'snowflake-connector-python',
            'google-cloud-bigquery',
            'google-cloud-bigtable',
            'google-cloud-storage',

        ],
        'playwright': [
            'playwright'
        ],
        'azure': [
            'azure-cosmos'
        ],
        'honeycomb': [
            'libhoney'
        ],
        'influxdb': [
            'influxdb_client'
        ],
        'newrelic': [
            'newrelic'
        ],
        'statsd': [
            'aio_statsd'
        ],
        'prometheus': [
            'prometheus-client',
            'prometheus-api-client'
        ],
        'cassandra': [
            'cassandra-driver'
        ],
        'datadog': [
            'datadog'
        ],
        'mongodb': [
            'motor'
        ],
        'redis': [
            'redis',
            'aioredis'
        ],
        'kafka': [
            'aiokafka'
        ],
        'sql': [
            'aiomysql',
            'psycopg2-binary',
            'aiopg',
            'sqlalchemy'
        ],
        'aws': [
            'boto3'
        ],
        'grpc': [
            'grpcio',
            'grpcio-tools'
        ],
        'graphql': [
            'gql'
        ],
        'snowflake': [
            'sqlalchemy',
            'snowflake-connector-python'
        ],
        'google': [
            'google-cloud-bigquery',
            'google-cloud-bigtable',
            'google-cloud-storage',
        ]
    },
    python_requires='>=3.8'
)
