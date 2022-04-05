import os
from setuptools import (
    setup,
    find_packages
)

current_directory = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(current_directory, 'README.md'), "r") as readme:
    package_description = readme.read()

setup(
    name="hedra",
    version="0.1.12",
    description="Powerful performance testing made easy.",
    long_description=package_description,
    long_description_content_type="text/markdown",
    author="Sean Corbett",
    author_email="sean.corbett@umconnect.edu",
    url="https://github.com/scorbettUM/hedra",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    install_requires=[
        'aiohttp[speedups]',
        'aiosonic',
        'httpx',
        'httpx[http2]',
        'pytz',
        'playwright',
        'tzlocal',
        'eventlet',
        'psutil',
        'grpcio',
        'grpcio-tools',
        'gql[all]>=3.0.0a6',
        'flask',
        'uwsgi',
        'alive-progress',
        'dill',
        'scipy',
        'art',
        'scikit-learn',
        'uvloop',
        'asyncio-pool',
        'playwright',
        'py3cli-tools',
        'py3-async-tools',
        'easy-logger-py',
        'broadkast',
        'statstream-py',
        'statserve',
        'aiopg',
        'prometheus-client',
        'prometheus-api-client',
        'psycopg2-binary',
        'motor',
        'datadog==0.42.0',
        'cassandra-driver',
        'redis',
        'aioredis',
        'aiokafka',
        'boto3',
        'google-cloud-storage',
        'snowflake-connector-python'
    ],
    entry_points = {
        'console_scripts': [
            'hedra=hedra.hedra:run',
            'hedra-server=hedra.run_uwsgi:run_uwsgi'
        ],
    },
    python_requires='>=3.8'
)
