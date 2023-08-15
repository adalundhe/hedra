'''
Cache module.
'''

import json
import os
from urllib import request
from pathlib import Path

from hedra.distributed.discovery.dns.core.record import (
    Record,
    RecordType,
    RecordTypesMap
)


__all__ = [
    'core_config',
    'get_name_cache',
    'get_root_servers',
]

CONFIG_DIR = os.environ.get('MERCURY_SYNC_DNS_CONFIG_DIR',
                            os.path.expanduser('~/.config/mercury_dns'))
os.makedirs(CONFIG_DIR, exist_ok=True)
CACHE_FILE = os.path.join(CONFIG_DIR, 'named.cache.txt')

try:
    with open(os.path.join(CONFIG_DIR, 'config.json')) as f:
        user_config = json.load(f)
except Exception:
    user_config = None

core_config = {
    'default_nameservers': [
        '8.8.8.8',
        '8.8.4.4',
    ],
}
if user_config is not None:
    core_config.update(user_config)
    del user_config


def get_nameservers():
    return []


def get_name_cache(
    url='ftp://rs.internic.net/domain/named.cache',
    filename=CACHE_FILE,
    timeout=10
):

    try:
        res = request.urlopen(url, timeout=timeout)

    except Exception:
        pass

    else:
        with open(filename, 'wb') as f:
            f.write(res.read())


def get_root_servers(filename=CACHE_FILE):

    if not os.path.isfile(filename):
        get_name_cache(filename=filename)

    if not os.path.isfile(filename):
        return
    for line in Path(filename).read_text().splitlines():

        if line.startswith(';'):
            continue

        parts = line.lower().split()
        if len(parts) < 4:
            continue

        name = parts[0].rstrip('.')

        types_map = RecordTypesMap()
        record_type = types_map.types_by_code.get(parts[2], RecordType.NONE)

        data_str = parts[3].rstrip('.')
        data = Record.create_rdata(record_type, data_str)
        yield Record(
            name=name,
            record_type=record_type,
            data=data,
            ttl=-1,
        )
