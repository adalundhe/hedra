def get_bits(
    num: int, 
    bit_len: int
):

    high = num >> bit_len
    low = num - (high << bit_len)

    return low, high