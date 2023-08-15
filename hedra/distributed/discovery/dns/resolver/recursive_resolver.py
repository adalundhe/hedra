import asyncio
import os
import pathlib
from urllib import request

from hedra.distributed.discovery.dns.core.cache import CacheNode
from hedra.distributed.models.dns_message import (
    DNSMessage,
    QueryType
)
from hedra.distributed.discovery.dns.core.url import URL
from hedra.distributed.discovery.dns.core.exceptions import (
    DNSError
)
from hedra.distributed.discovery.dns.core.nameservers import NameServer
from hedra.distributed.discovery.dns.core.record import (
    Record,
    RecordType, 
    RecordTypesMap
)
from hedra.distributed.discovery.dns.core.record.record_data_types import (
    CNAMERecordData,
    SOARecordData,
    NSRecordData
)
from hedra.distributed.env import (
    Env, 
    RegistrarEnv, 
    load_env
)
from hedra.distributed.env.time_parser import TimeParser
from typing import Tuple, Optional, List
from .base_resolver import BaseResolver
from .memoizer import Memoizer



class RecursiveResolver(BaseResolver):

    memoizer = Memoizer()

    def __init__(
        self, 
        host: str,
        port: int,
        instance_id: str,
        env: Env,
        cache: CacheNode = None
    ):
        super().__init__(
            host,
            port,
            instance_id,
            env,
            cache=cache
        )

        self.types_map = RecordTypesMap()
        self._nameserver_urls: List[str] = []

        registrar_env: RegistrarEnv = load_env(RegistrarEnv)

        self._maximum_tries = registrar_env.MERCURY_SYNC_RESOLVER_MAXIMUM_TRIES

    def add_nameserver(
        self,
        urls: List[str]
    ):
        self._nameserver_urls.extend(urls)

        for url in urls:

            self.cache.add(
                fqdn=url,
                record_type=RecordType.NS,
                data=NSRecordData(url)
            )

        nameserver = NameServer(urls)

        return nameserver.data
        
    def load_nameserver_cache(
        self,
        url: str='ftp://rs.internic.net/domain/named.cache',
        cache_file: str=os.path.join(
            os.getcwd(), 
            'named.cache.txt'
        ),
        timeout: Optional[int]=None
    ):
        
        if not os.path.isfile(cache_file):
            try:
                res = request.urlopen(
                    url, 
                    timeout=timeout
                )

                with open(cache_file, 'wb') as f:
                    f.write(res.read())

            except Exception:
                return

        cache_data = pathlib.Path(
            cache_file
        ).read_text().splitlines()

        for line in cache_data:
            if line.startswith(';'):
                continue
            parts = line.lower().split()
            if len(parts) < 4:
                continue

            name = parts[0].rstrip('.')
            # parts[1] (expires) is ignored
            record_type = self.types_map.types_by_name.get(
                parts[2], 
                RecordType.NONE
            )

            data_str = parts[3].rstrip('.')


            data = Record.create_rdata(
                record_type, 
                data_str
            )
            
            record = Record(
                name=name,
                record_type=record_type,
                data=data,
                ttl=-1,
            )

            self.cache.add(record=record)

    async def _query(
        self, 
        fqdn: str, 
        record_type: int,
        skip_cache: bool=False
    ) -> DNSMessage:
        
        current_try_count = 0

        return await self._query_tick(
            fqdn, 
            record_type, 
            skip_cache,
            current_try_count
        )

    def _get_matching_nameserver(self, fqdn: str):
        '''Return a generator of parent domains'''

        hosts: List[URL] = self._nameserver_urls
        empty = True

        while fqdn and empty:
            if fqdn in ('in-addr.arpa', ):
                break
            _, _, fqdn = fqdn.partition('.')

            for rec in self.cache.query(fqdn, RecordType.NS):
                record_data: NSRecordData = rec.data
                host = record_data.data

                url = URL(
                    host,
                    port=self.client.port
                )

                if url.ip_type is None:
                    # host is a hostname instead of IP address

                    for res in self.cache.query(
                        host, 
                        self.nameserver_types
                    ):
                        hosts.append(
                            URL(
                                res.data.data,
                                port=self.client.port
                            )
                        )

                        empty = False

                else:
                    hosts.append(url)
                    empty = False

        return NameServer(hosts)

    @memoizer.memoize_async(
        lambda _, fqdn, record_type, skip_cache: (fqdn, record_type)
    )
    async def _query_tick(
        self, 
        fqdn: str, 
        record_type: int,
        skip_cache: bool,
        current_try_count: int
    ):
        
        msg = DNSMessage()
        msg.query_domains.append(
            Record(
                query_type=QueryType.REQUEST, 
                name=fqdn, 
                record_type=record_type
            )
        )

        has_result = False

        if skip_cache is False:
            has_result, fqdn = self.query_cache(msg, fqdn, record_type)

        last_err = None
        nameserver = self._get_matching_nameserver(fqdn)

        while not has_result and current_try_count < self._maximum_tries:

            current_try_count += 1

            for url in nameserver.iter():
                try:
                    has_result, fqdn, nsips = await self._query_remote(
                        msg, 
                        fqdn, 
                        record_type, 
                        url,
                        current_try_count
                    )

                    nameserver = NameServer(
                        self.client.port,
                        nameservers=nsips
                    )

                except Exception as err:
                    last_err = err

                else:
                    break
            else:
                raise last_err or Exception('Unknown error')


        assert has_result, 'Maximum nested query times exceeded'

        return msg

    async def _query_remote(
        self, 
        msg: DNSMessage, 
        fqdn: str, 
        record_type: RecordType,
        url: URL,
        current_try_count: int
    ):
        
        result: DNSMessage = await self.request(
            fqdn, 
            msg, 
            url
        )

        if result.query_domains[0].name != fqdn:
            raise DNSError(-1, 'Question section mismatch')
        
        assert result.query_result_code != 2, 'Remote server fail'

        self.cache_message(result)

        has_cname = False
        has_result = False
        has_ns = False

        for rec in result.query_answers:
            msg.query_answers.append(rec)

            if isinstance(rec.data, CNAMERecordData):
                fqdn = rec.data.data
                has_cname = True

            if rec.record_type != RecordType.CNAME or record_type in (
                RecordType.ANY, 
                RecordType.CNAME
            ):
                has_result = True

        for rec in result.query_namservers:
            if rec.record_type in (
                RecordType.NS, 
                RecordType.SOA
            ):
                has_result = True

            else:
                has_ns = True

        if not has_cname and not has_ns:
            # Not found, return server fail since we are not authorative
            msg = DNSMessage(
                **msg.dict(),
                query_result_code=2
            )

            has_result = True
        if has_result:
            return has_result, fqdn, []

        # Load name server IPs from res.ar
        namespace_ip_address_map = {}

        for record in result.query_additional_records:
            if record.record_type in self.nameserver_types:
                namespace_ip_address_map[(rec.name, record.record_type)] = rec.data.data

        hosts = []
        for record in result.query_namservers:

            if isinstance(record.data, SOARecordData):
                hosts.append(record.data.mname)

            elif isinstance(record.data, NSRecordData):
                hosts.append(record.data.data)

        namespace_ips = []

        for host in hosts:
            for record_type in self.nameserver_types:
                ip = namespace_ip_address_map.get((host, record_type))

                if ip is not None:
                    namespace_ips.append(ip)

        # Usually name server IPs will be included in res.ar.
        # In case they are not, query from remote.
        if len(namespace_ips) < 1 and len(hosts) > 0:

            current_try_count += 1

            for record_type in self.nameserver_types:
                for host in hosts:
                    try:
                        query_tick_result: Tuple[DNSMessage, bool] = await asyncio.shield(
                            self._query_tick(
                                host, 
                                record_type, 
                                False,
                                current_try_count
                            )
                        )

                        (
                            ns_res, 
                            _
                        ) = query_tick_result

                    except Exception:
                        pass

                    else:
                        for rec in ns_res.query_answers:
                            if rec.record_type == record_type:
                                namespace_ips.append(rec.data.data)
                                break

                if len(namespace_ips) > 0:
                    break

        return has_result, fqdn, namespace_ips

