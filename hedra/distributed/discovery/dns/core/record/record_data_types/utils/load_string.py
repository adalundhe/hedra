def load_string(
    buffer: bytes, 
    offset: int
):
    '''Load a character string from packed data.'''
    length = buffer[offset]
    offset += 1
    data = buffer[offset:offset + length]
    return offset + length, data
