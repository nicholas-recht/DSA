
# config settings
database = "database.db"
config_file = "config.dsa"
bufsize = 1024
restart_window = 3.0
wait_interval = .1
command_port = 18981
slave_connect_wait = 1.0
master_continuous_wait = 1.0



def i_from_bytes(val):
    """
    Converts the given byte array to an integer value and returns the result
    :param val:
    :return:
    """
    return int.from_bytes(val, byteorder="little", signed=True)


def i_to_bytes(val):
    """
    Converts the given integer to a byte array and returns the result
    :param val:
    :return:
    """
    return int.to_bytes(val, length=val.bit_length() // 8 + 1, byteorder="little", signed=True)


def s_from_bytes(val):
    """
    Converts the given byte array into its string representation and returns the result
    :param val:
    :return:
    """
    return val.decode("ascii")


def s_to_bytes(val):
    """
    Converts the given string to a byte array and returns the result
    :param val:
    :return:
    """
    return val.encode("ascii")
