from typing import List, Tuple, Union
from hedra.distributed.discovery.dns.core.url import (
    URL,
    InvalidHost,
    InvalidIP

)
from hedra.distributed.discovery.dns.core.cache import CacheNode
from hedra.distributed.models.dns import (
    DNSEntry,
    DNSMessage,
    QueryType
)
from hedra.distributed.discovery.dns.core.record import RecordType
from hedra.distributed.discovery.dns.core.record import Record
from hedra.distributed.discovery.dns.core.record.record_data_types import (
    CNAMERecordData,
    NSRecordData
)
from .memoizer import Memoizer


class CacheResolver:
    zone_domains = []
    nameserver_types = [
        RecordType.A
    ]
    memoizer=Memoizer()

    def __init__(    
        self,
        port: int,
        cache: CacheNode = None,
        query_timeout: float = 3.0,
        request_timeout: float = 5.0
    ):
        self.port = port
        self._queries = {}
        self.cache = cache or CacheNode()
        self.request_timeout = request_timeout
        self.query_timeout = query_timeout

    def cache_message(self, entry: DNSEntry):

        for _, record in entry.to_record_data():
            if entry.time_to_live > 0 and record.rtype != RecordType.SOA:
                self.cache.add(record=record)

    def set_zone_domains(self, domains: List[str]):
        self.zone_domains = [
            domain.lstrip('.') for domain in domains
        ]

    async def _query(self, _fqdn: str, _record_type: RecordType) -> Tuple[DNSMessage, bool]:
        raise NotImplementedError

    @memoizer.memoize_async(
            lambda _, fqdn, record_type, skip_cache: (fqdn, record_type)
    )
    async def query_local(
        self,
        fqdn: str,
        record_type: RecordType=RecordType.ANY
    ) -> Tuple[DNSMessage, bool]:
        
        if fqdn.endswith('.'):
            fqdn = fqdn[:-1]

        if record_type == RecordType.ANY:
            try:

                url = URL(
                    fqdn,
                    port=self.port
                )

                ptr_name = url.to_ptr()

            except (InvalidHost, InvalidIP):
                pass

            else:
                fqdn = ptr_name
                record_type = RecordType.PTR

        msg = DNSMessage()
        msg.query_domains.append(
            Record(
                QueryType.REQUEST, 
                name=fqdn, 
                record_type=record_type
            )
        )

        has_result = False
        has_result, fqdn = self.query_cache(msg, fqdn, record_type)

        return msg, has_result

    def _add_cache_cname(self, msg: DNSMessage, fqdn: str) -> Union[str, None]:

        for cname in self.cache.query(fqdn, RecordType.CNAME):

            msg.query_answers.append(cname.copy(name=fqdn))
            if isinstance(cname.data, CNAMERecordData):
                return cname.data.data

    def _add_cache_record_type(
        self, 
        msg: DNSMessage, 
        fqdn: str, 
        record_type: RecordType
    ) -> bool:
        '''Query cache for records other than CNAME and add to result msg.
        '''
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
        fqdn: str, 
        record_type: RecordType
    ) -> Tuple[DNSMessage, bool]:
        
        if fqdn.endswith('.'):
            fqdn = fqdn[:-1]

        if record_type == RecordType.ANY:
            try:

                url = URL(
                    fqdn,
                    port=self.port
                )

                ptr_name = url.to_ptr()

            except (InvalidHost, InvalidIP):
                pass

            else:
                fqdn = ptr_name
                record_type = RecordType.PTR

        msg = DNSMessage()
        msg.query_domains.append(
            Record(
                QueryType.REQUEST, 
                name=fqdn, 
                record_type=record_type
            )
        )

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
            has_result = self._add_cache_record_type(
                msg, 
                fqdn, 
                record_type
            ) or has_result

        if any(('.' + fqdn).endswith(root) for root in self.zone_domains):
            if not has_result:
                msg.r = 3
                has_result = True

            msg = DNSMessage(
                **msg.dict(),
                query_authoritative_answer=1
            )
        # fqdn may change due to CNAME
        return msg, has_result
