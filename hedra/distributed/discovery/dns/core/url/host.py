from typing import Union


class Host:
    hostname: str
    port: Union[int, None]
    username: Union[str, None]
    password: Union[str, None]

    def __init__(
        self, 
        netloc: str
    ):
        userinfo, _, host = netloc.rpartition('@')
        if host.count(':') == 1 or '[' in host:
            hostname, _, port = host.rpartition(':')
            port = int(port)
        else:
            hostname, port = host, None
        if hostname.startswith('[') and hostname.endswith(']'):
            hostname = hostname[1:-1]
        if userinfo:
            username, _, password = userinfo.partition(':')
        else:
            username = password = None

        self.netloc = netloc
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password

    @property
    def host(self):
        host = f'[{self.hostname}]' if ':' in self.hostname else self.hostname
        if self.port:
            host = f'{host}:{self.port}'
        return host

    def __str__(self):
        userinfo = ''
        if self.username:
            userinfo += self.username
            if self.password:
                userinfo += ':' + self.password
            userinfo += '@'
        return userinfo + self.host
