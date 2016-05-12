
def i_from_bytes(val):
    """
    Converts the given byte array to an integer value and returns the result
    :param val:
    :return:
    """
    return int.from_bytes(val, byteorder="little")


def i_to_bytes(val):
    """
    Converts the given integer to a byte array and returns the result
    :param val:
    :return:
    """
    return int.to_bytes(val, length=val.bit_length() // 8 + 1, byteorder="little")