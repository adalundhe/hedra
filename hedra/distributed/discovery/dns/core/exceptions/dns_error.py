class DNSError(Exception):
    errors = {
        1: 'Format error: bad request',
        2: 'Server failure: error occurred',
        3: 'Name error: not exist',
        4: 'Not implemented: query type not supported',
        5: 'Refused: policy reasons'
    }

    def __init__(self, code: int, message: str = None):
        message = self.errors.get(code,
                                  message) or 'Unknown reply code: %d' % code
        super().__init__(message)
        self.code = code
