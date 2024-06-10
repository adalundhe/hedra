class Timeouts:

    __slots__ = (
        'connect_timeout',
        'read_timeout',
        'write_timeout',
        'request_timeout',
        'total_time'
    )

    def __init__(
        self, 
        connect_timeout: int=10, 
        read_timeout: int=5, 
        write_timeout: int=5,
        request_timeout: int=60,
        total_time: int=60
    ) -> None:
        
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.request_timeout = request_timeout
        self.total_time = total_time

        if self.request_timeout > self.total_time:
            self.request_timeout = self.total_time

        sum_timeout = connect_timeout + read_timeout + write_timeout
        
        if sum_timeout > self.request_timeout:
            raise Exception('Err. total connect, read, and write timeout cannot exceed request timeout.')
        