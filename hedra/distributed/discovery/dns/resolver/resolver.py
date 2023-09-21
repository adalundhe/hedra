import asyncio
from hedra.distributed.discovery.dns.core.record import (
    RecordType,
    RecordTypesMap
)
from hedra.distributed.discovery.dns.core.record.record_data_types import RecordData
from hedra.distributed.discovery.dns.core.url import URL
from hedra.distributed.env import Env
from hedra.distributed.models.dns import DNSMessage
from typing import Literal, Optional, List, Tuple, Callable, Union
from .proxy_resolver import ProxyResolver
from .recursive_resolver import RecursiveResolver


Proxy = List[
    Tuple[
        Union[
            Callable[
                [str],
                bool
            ],
            str,
            None
        ], 
        str
    ]
]


class DNSResolver:

    def __init__(
        self,
        host: str,
        port: int,
        instance_id: str,
        env: Env,
        resolver: Literal["proxy", "recursive"]="proxy",
        proxies: Optional[
            List[Proxy]
        ]=None
    ) -> None:
        
        if resolver == "proxy":
            self.resolver = ProxyResolver(
                host,
                port,
                instance_id,
                env,
                proxies=proxies
            )

        else:
            self.resolver = RecursiveResolver(
                host,
                port,
                instance_id,
                env
            )

        self.types_map = RecordTypesMap()

    def add_to_cache(
        self,
        domain: str,
        record_type: RecordType,
        data: RecordData,
        ttl: Union[int, float]=-1
    ):
        self.resolver.cache.add(
            fqdn=domain,
            record_type=record_type,
            data=data,
            ttl=ttl
        )

    def add_nameservers(
        self,
        urls: List[str]
    ):
        return self.resolver.add_nameserver(urls)

    def set_proxies(
        self,
        proxies: List[Proxy]
    ):
        if isinstance(self.resolver, ProxyResolver):
            self.resolver.set_proxies(proxies)

    def download_common(self):
        if isinstance(self.resolver, RecursiveResolver):
            self.resolver.load_nameserver_cache()

    async def connect_nameservers(
        self,
        urls: List[URL],
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
    ):
        
        await asyncio.gather(*[
            self.resolver.client.connect_client(
                url,
                cert_path=cert_path,
                key_path=key_path
            ) for url in urls
        ])

    async def query(
        self,
        domain_name: str,
        record_type: RecordType=RecordType.SRV,
        skip_cache: bool=False
    ) -> Tuple[DNSMessage, bool]:

        try:
            result = await self.resolver.query(
                domain_name,
                record_type=record_type,
                skip_cache=skip_cache
            )

            return result, True
        
        except asyncio.TimeoutError:
            return DNSMessage(), False