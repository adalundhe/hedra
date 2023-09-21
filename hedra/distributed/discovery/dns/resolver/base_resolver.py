import asyncio
from typing import List, Tuple, Union
from hedra.distributed.discovery.dns.core.url import (
    URL,
    InvalidHost,
    InvalidIP

)
from hedra.distributed.discovery.dns.core.cache import CacheNode
from hedra.distributed.env import (
    Env, 
    RegistrarEnv, 
    load_env
)
from hedra.distributed.env.time_parser import TimeParser
from hedra.distributed.models.dns import (
    DNSEntry,
    DNSMessage
)
from hedra.distributed.discovery.dns.core.exceptions import (
    DNSError
)
from hedra.distributed.discovery.dns.core.record import RecordType
from hedra.distributed.discovery.dns.core.record.record_data_types import (
    CNAMERecordData,
    NSRecordData
)
from hedra.distributed.discovery.dns.request.dns_client import DNSClient
from .memoizer import Memoizer


class BaseResolver:
    zone_domains = []
    nameserver_types = [
        RecordType.A
    ]
    memoizer=Memoizer()

    def __init__(    
        self,
        host: str,
        port: int,
        instance_id: str,
        env: Env,
        cache: CacheNode = None
    ):
        self.host = host
        self.port = port
        self._queries = {}
        self.cache = cache or CacheNode()
        self.client = DNSClient(
            host,
            port,
            instance_id,
            env
        )

        registrar_env: RegistrarEnv = load_env(RegistrarEnv)
        
        self._request_timeout = TimeParser(
            registrar_env.MERCURY_SYNC_RESOLVER_REQUEST_TIMEOUT
        ).time

    def cache_message(self, query: DNSEntry):
        for _, record in query.to_record_data():
            if query.time_to_live > 0 and record.rtype != RecordType.SOA:
                self.cache.add(record=record)

    def set_zone_domains(self, domains: List[str]):
        self.zone_domains = [
            domain.lstrip('.') for domain in domains
        ]

    async def _query(self, _fqdn: str, _record_type: RecordType) -> DNSMessage:
        raise NotImplementedError

    async def query(
        self,
        fqdn: str,
        record_type: RecordType=RecordType.ANY,
        skip_cache: bool=False
    ):
        
        if fqdn.endswith('.'):
            fqdn = fqdn[:-1]

        if record_type == RecordType.ANY:
            try:
                addr = URL(
                    fqdn,
                    port=self.port
                )

                ptr_name = addr.to_ptr()

            except (InvalidHost, InvalidIP):
                pass

            else:
                fqdn = ptr_name
                record_type = RecordType.PTR

        try:

            return await asyncio.wait_for(
                self._query(
                    fqdn, 
                    record_type,
                    skip_cache
                ),
                timeout=self._request_timeout
            )
        
        except asyncio.TimeoutError:
            return DNSMessage()

    async def request(
        self, 
        fqdn: str, 
        message: DNSMessage, 
        url: URL
    ) -> DNSMessage:

        result = await self.client.send(fqdn, message, url)

        if len(result.query_domains) < 1:
            return False, fqdn, []

        if result.query_domains[0].name != fqdn:
            raise DNSError(-1, 'Question section mismatch')
        
        assert result.query_result_code != 2, 'Remote server fail'

        self.cache_message(result)

        return result

    def _add_cache_cname(self, msg: DNSMessage, fqdn: str) -> Union[str, None]:
        for cname in self.cache.query(fqdn, RecordType.CNAME):
            msg.query_answers.append(cname.copy(name=fqdn))
            if isinstance(cname.data, CNAMERecordData):
                return cname.data.data

    def _add_cache_cname(
        self, 
        msg: DNSMessage, 
        fqdn: str
    ) -> Union[str, None]:
        
        for cname in self.cache.query(fqdn, RecordType.CNAME):
            msg.query_answers.append(cname.copy(name=fqdn))
            if isinstance(cname.data, CNAMERecordData):
                return cname.data.data
            
    def _add_cache_qtype(
        self, 
        msg: DNSMessage, 
        fqdn: str, 
        record_type: RecordType
    ) -> bool:
        if record_type == RecordType.CNAME:
            return False
        
        has_result = False
        for rec in self.cache.query(fqdn, record_type):

            if isinstance(rec.data, NSRecordData):
                a_res = list(self.cache.query(
                    rec.data.data, (
                        RecordType.A,
                        RecordType.AAAA
                    )
                ))
                
                if a_res:
                    msg.query_additional_records.extend(a_res)
                    msg.query_namservers.append(rec)
                    has_result = True
            else:
                msg.query_answers.append(rec.copy(name=fqdn))
                has_result = True

        return has_result

    def _add_cache_record_type(
        self, 
        msg: DNSMessage, 
        fqdn: str, 
        record_type: RecordType
    ) -> bool:
        
        if record_type == RecordType.CNAME:
            return False
        
        has_result = False
        for rec in self.cache.query(fqdn, record_type):

            if isinstance(rec.data, NSRecordData):
                records = list(self.cache.query(
                    rec.data.data, (
                        RecordType.A,
                        RecordType.AAAA
                    )
                ))
                
                if records:
                    msg.query_additional_records.extend(records)
                    msg.query_namservers.append(rec)
                    has_result = True
            else:
                msg.query_answers.append(rec.copy(name=fqdn))
                has_result = True

        return has_result
 
    def query_cache(
        self, 
        msg: DNSMessage,
        fqdn: str, 
        record_type: RecordType
    ):

        cnames = set()

        while True:
            cname = self._add_cache_cname(msg, fqdn)
            if not cname: 
                break

            if cname in cnames:
                # CNAME cycle detected
                break

            cnames.add(cname)
            # RFC1034: If a CNAME RR is present at a node, no other data should be present
            fqdn = cname

        has_result = bool(cname) and record_type in (RecordType.CNAME, RecordType.ANY)

        if record_type != RecordType.CNAME:
            has_result = self._add_cache_qtype(msg, fqdn, record_type) or has_result

        if any(('.' + fqdn).endswith(root) for root in self.zone_domains):
            if not has_result:
                msg.query_result_code = 3
                has_result = True

            msg.query_authoritative_answer = 1
        # fqdn may change due to CNAME
        return has_result, fqdn