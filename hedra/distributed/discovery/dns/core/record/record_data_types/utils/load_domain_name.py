def load_domain_name(
    buffer: bytes, 
    offset: int
):
    parts = []
    cursor = None
    data_len = len(buffer)
    visited = set()

    while offset < data_len:

        if offset in visited:
            raise Exception(buffer, offset, 'Pointer loop detected')
        
        visited.add(offset)
        length = buffer[offset]
        offset += 1

        if length == 0:

            if cursor is None:
                cursor = offset

            break

        if length >= 0xc0:

            if cursor is None:
                cursor = offset + 1

            offset = (length - 0xc0) * 256 + buffer[offset]

            continue

        parts.append(buffer[offset:offset + length])
        offset += length

    if cursor is None:
        raise Exception(buffer, offset, 'Bad data')
    
    data = b'.'.join(parts).decode()

    return cursor, data