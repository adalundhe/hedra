
from hedra.distributed.discovery.dns.core.cache import CacheNode
from hedra.distributed.models.dns import (
    DNSMessage,
    QueryType
)
from hedra.distributed.discovery.dns.core.config import core_config
from hedra.distributed.discovery.dns.core.nameservers import NameServer
from hedra.distributed.discovery.dns.core.record import (
    Record,
    RecordType,
    RecordTypesMap
)
from hedra.distributed.env import Env
from typing import Callable, List, Tuple, Optional, Union
from .base_resolver import BaseResolver
from .memoizer import Memoizer



Proxy = Tuple[
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

NameServerPair = Tuple[
    Union[
        Callable[
            [str],
            bool
        ],
        None
    ],
    NameServer
]


class ProxyResolver(BaseResolver):

    default_nameservers = core_config['default_nameservers']
    memoizer = Memoizer()

    def __init__(
        self, 
        host: str,
        port: int,
        instance_id: str,
        env: Env,
        cache: CacheNode = None,
        proxies: Optional[List[Proxy]]=None
    ):
        super().__init__(
            host,
            port,
            instance_id,
            env,
            cache=cache
        )

        if proxies is None:
            proxies = self.default_nameservers

        self.types_map = RecordTypesMap()
        self._nameserver_pairs = self.set_proxies(proxies)

    def _get_matching_nameserver(self, fqdn):

        for nameserver_test, nameserver in self._nameserver_pairs:
            if nameserver_test is None or nameserver_test(fqdn):
                return nameserver

        return NameServer([])
    
    def add_nameserver(
        self,
        urls: List[str]
    ):
        namserver = NameServer(urls)

        self._nameserver_pairs.append((
            None,
            namserver
        ))

        return namserver.data

    @staticmethod
    def build_tester(rule) ->  Callable[
        [str],
        bool
    ]:

        if rule is None or callable(rule):
            return rule
        
        assert isinstance(rule, str)

        if rule.startswith('*.'):
            suffix = rule[1:]

            return lambda d: d.endswith(suffix)
        
        return lambda d: d == rule

    def set_proxies(
        self, 
        proxies: List[Proxy]
    ):

        nameserver_pairs: List[NameServerPair] = []
        fallback: List[str] = []

        if proxies:
            for item in proxies:

                if isinstance(item, str):
                    fallback.append(item)
                    continue

                test, nameserver = item
                if test is None:
                    fallback.extend(nameserver)
                    continue

                nameserver_pairs.append(
                    (
                        self.build_tester(test), 
                        NameServer([nameserver])
                    )
                )

        if fallback:
            nameserver_pairs.append(
                (
                    None, 
                    NameServer(fallback)
                )
            )

        return nameserver_pairs

    @memoizer.memoize_async(
            lambda _, fqdn, record_type, skip_cache: (fqdn, record_type)
    )
    async def _query(
        self, 
        fqdn: str, 
        record_type: RecordType,
        skip_cache: bool
    ):

        msg = DNSMessage()
        msg.query_domains.append(
            Record(
                QueryType.REQUEST, 
                name=fqdn, 
                record_type=record_type
            )
        )

        has_result = False

        if skip_cache is False:
            has_result, fqdn = self.query_cache(msg, fqdn, record_type)

        while not has_result:
            nameserver = self._get_matching_nameserver(fqdn)

            for addr in nameserver.iter():
                try:
                    res = await self.request(fqdn, msg, addr)

                except:
                    nameserver.fail(addr)
                    raise

                else:
                    nameserver.success(addr)
                    self.cache_message(res)
                    msg.query_answers.extend(res.query_answers)
                    has_result = True

                    break

        return msg

