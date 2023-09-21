import asyncio
import socket
from hedra.distributed.discovery.dns.core.url import URL
from hedra.distributed.env import load_env, Env
from hedra.distributed.env.time_parser import TimeParser
from hedra.distributed.hooks import (
    client,
    server
)
from hedra.distributed.models.dns import (
    DNSMessage,
    DNSMessageGroup,
    DNSEntry,
    Service
)
from hedra.distributed.service.controller import Controller
from hedra.distributed.discovery.dns.core.record import (
    Record
)
from hedra.distributed.discovery.dns.core.random import RandomIDGenerator

from hedra.distributed.discovery.dns.resolver import DNSResolver
from hedra.distributed.types import Call
from typing import (
    Optional, 
    List,
    Union,
    Dict,
    Tuple
)


class Registrar(Controller):

    def __init__(
        self,
        host: str,
        port: int,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        workers: int=0,
        env: Env=None
    ) -> None:

        if env is None:
            env = load_env(Env)
    
        super().__init__(
            host,
            port,
            cert_path=cert_path,
            key_path=key_path,
            env=env,
            workers=workers,
            engine='async'
        )

        self.resolver = DNSResolver(
            host,
            port,
            self._instance_id,
            self._env
        )

        self.random_id_generator = RandomIDGenerator()

        self._nameservers: List[URL] = []
        self._next_nameserver_idx = 0
        self._connected_namservers: Dict[Tuple[str, int], bool] = {}
        self._connected_domains: Dict[str, bool] = {}

    def add_entries(
        self,
        entries: List[DNSEntry]
    ):

        for entry in entries:
            for domain, record in entry.to_record_data():
                
                self.resolver.add_to_cache(
                    domain,
                    record.rtype,
                    record
                )

    async def add_nameservers(
        self,
        urls: List[str]
    ):
        urls = self.resolver.add_nameservers(urls)

        await self.resolver.connect_nameservers(
            urls,
            cert_path=self.cert_path,
            key_path=self.key_path
        )

        self._nameservers.extend(urls)

    def _next_nameserver_url(self) -> Union[URL, None]:

        if len(self._nameservers) > 0:

            namserver_url = self._nameservers[self._next_nameserver_idx]

            self._next_nameserver_idx = (
                self._next_nameserver_idx + 1
            )%len(self._nameservers)

            return namserver_url

    @server()
    async def update_registered(
        self,
        shard_id: int,
        registration: DNSMessage
    ):
        for record in registration.query_domains:
            self.resolver.add_to_cache(
                record.name,
                record.record_type,
                record.data
            )

        return registration


    @server()
    async def resolve_query(
        self,
        shard_id: int,
        query: DNSMessage
    ) -> Call[DNSMessage]:
        
        messages: List[DNSMessage] = []
        
        for record in query.query_domains:
                  
            dns_message, has_result = await self.resolver.query(
                record.name,
                record_type=record.record_type
            )

            if has_result is False:
                # TODO: Query using client.
                pass

            dns_data = dns_message.to_data()
            dns_data.update({
                'query_id': query.query_id,
                'has_result': has_result
            })

            response = DNSMessage(**dns_data)

            messages.append(response)

        return DNSMessageGroup(
            messages=messages
        )
            
    @client('resolve_query')
    async def submit_query(
        self,
        host: str,
        port: int,
        entry: DNSEntry
    ) -> Call[DNSMessageGroup]:
   
        return DNSMessage(
            host=host,
            port=port,
            query_domains=[
                Record(
                    name=domain,
                    record_type=record.rtype,
                    data=record,
                    ttl=entry.time_to_live

                ) for domain, record in entry.to_record_data()
            ]
        )
    
    @client('update_registered')
    async def submt_registration(
        self,
        host: str,
        port: int,
        entry: DNSEntry
    ) -> Call[DNSMessage]:
        return DNSMessage(
            host=host,
            port=port,
            query_domains=[
                Record(
                    name=domain,
                    record_type=record.rtype,
                    data=record,
                    ttl=entry.time_to_live

                ) for domain, record in entry.to_record_data()
            ]
        )
    
    async def query(
        self,
        entry: DNSEntry
    ) -> List[DNSEntry]:

        nameserver_url = self._next_nameserver_url()

        host = nameserver_url.host
        port = nameserver_url.port

        if nameserver_url.ip_type is not None:
            host = socket.gethostbyname(nameserver_url.host)

        if not self._connected_namservers.get((host, port)):
            
            await self.start_client(
                DNSMessage(
                    host=host,
                    port=port
                )
            )

            self._connected_namservers[(host, port)] = True

        _, results = await self.submit_query(
            host,
            port,
            entry
        )

        entries: List[DNSEntry]=[]

        for message in results.messages:
            for answer in message.query_answers:

                entries.append(
                    DNSEntry.from_record_data(
                        answer.name,
                        answer.data
                    )
                )
                
        return entries
    
    async def register(
        self,
        entry: DNSEntry
    ) -> List[DNSEntry]:
        
        nameserver_url = self._next_nameserver_url()

        host = nameserver_url.host
        port = nameserver_url.port

        if nameserver_url.ip_type is not None:
            host = socket.gethostbyname(nameserver_url.host)

        if not self._connected_namservers.get((host, port)):
            
            await self.start_client(
                DNSMessage(
                    host=host,
                    port=port
                )
            )

            self._connected_namservers[(host, port)] = True

        _, results = await self.submt_registration(
            host,
            port,
            entry
        )

        entries: List[DNSEntry]=[]

        for answer in results.query_domains:

                entries.append(
                    DNSEntry.from_record_data(
                        answer.name,
                        answer.data
                    )
                )
                
        return entries\
        
    async def discover(
        self, 
        url: str,
        expected: Optional[int]=None,
        timeout: Optional[str]=None
    ):

        services_data: Dict[str, Dict[str, Union[str, int,  Dict[str, str]]]] = {}
        services: Dict[str, Service] = {}

        

        if expected and timeout:

            poll_timeout = TimeParser(timeout).time

            return await asyncio.wait_for(
                self.poll_for_services(
                    url,
                    expected
                ),
                timeout=poll_timeout
            )
        
        else:
            return await self.get_services(url)
    
    async def poll_for_services(
        self,
        url: str,
        expected: int
    ):
        services_data: Dict[str, Dict[str, Union[str, int,  Dict[str, str]]]] = {}
        services: Dict[str, Service] = {}

        discovered = 0

        while discovered < expected:
            
            ptr_records = await self.get_ptr_records(url)

            srv_records = await self.get_srv_records(ptr_records)
            txt_records = await self.get_txt_records(ptr_records)  


            for record in srv_records:
                service_url = record.to_domain(record.record_type.name)

                services_data[service_url] = {
                    "service_instance": record.instance_name,
                    "service_name": record.service_name,
                    "service_protocol": record.domain_protocol,
                    'service_url': service_url,
                    'service_ip': record.domain_targets[0],
                    'service_port': record.domain_port,
                    'service_context': {}
                }

            for record in txt_records:
                service_url = record.domain_name

                services_data[service_url]['service_context'].update(record.domain_values)

            for service_url, data in services_data.items():
                services[service_url] = Service(**data)
            
            discovered = len(services)

        return list(services.values())

    async def get_services(self, url: str):

        services_data: Dict[str, Dict[str, Union[str, int,  Dict[str, str]]]] = {}
        services: Dict[str, Service] = {}

        ptr_records = await self.get_ptr_records(url)

        srv_records = await self.get_srv_records(ptr_records)
        txt_records = await self.get_txt_records(ptr_records)  


        for record in srv_records:
            service_url = record.to_domain(record.record_type.name)

            services_data[service_url] = {
                "service_instance": record.instance_name,
                "service_name": record.service_name,
                "service_protocol": record.domain_protocol,
                'service_url': service_url,
                'service_ip': record.domain_targets[0],
                'service_port': record.domain_port,
                'service_context': {}
            }


        for record in txt_records:
            service_url = record.domain_name

            services_data[service_url]['service_context'].update(record.domain_values)

        for service_url, data in services_data.items():
            services[service_url] = Service(**data)
        
        return list(services.values())  

    async def get_ptr_records(
        self,
        url: str
    ):

        (
            service_name, 
            domain_protocol,
            domain_name
        ) = DNSEntry.to_ptr_segments(url)

    
        return await self.query(
            DNSEntry(
                service_name=service_name,
                domain_protocol=domain_protocol,
                domain_name=domain_name,
                record_types=["PTR"]
            )
        )
    
    async def get_srv_records(
        self,
        ptr_records: List[DNSEntry]
    ):
        
        srv_records: List[List[DNSEntry]] = await asyncio.gather(*[
            self.query(
                DNSEntry(
                    instance_name=entry.instance_name,
                    service_name=entry.service_name,
                    domain_protocol=entry.domain_protocol,
                    domain_name=entry.domain_name,
                    record_types=["SRV"]
                )
            ) for entry in ptr_records
        ], return_exceptions=True)

        service_records: List[DNSEntry] = []

        for results in srv_records:
            service_records.extend(results)

        return service_records
    
    async def get_txt_records(
        self,
        ptr_records: List[DNSEntry]
    ):
        txt_records = await asyncio.gather(*[
            self.query(
                DNSEntry(
                    instance_name=entry.instance_name,
                    service_name=entry.service_name,
                    domain_protocol=entry.domain_protocol,
                    domain_name=entry.domain_name,
                    record_types=["TXT"]
                )
            ) for entry in ptr_records
        ], return_exceptions=True)

        text_records: List[DNSEntry] = []
        for results in txt_records:
            text_records.extend(results)

        return text_records
