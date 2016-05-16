import os
import sys
import util
import socket
import time


class Slave:
    def __init__(self, address=None, port=None, storage_space=None, storage_loc=None):
        # member variables
        self.id = -1
        self.socket = None
        self.address = None
        self.port = 0
        self.storage_space = 0
        self.storage_loc = ""

        # if all params are None then read from a config file
        if address is None and port is None and storage_space is None and storage_loc is None:
            self.read_config_settings()
        else:
            self.address = address
            self.port = port
            self.storage_space = storage_space
            self.storage_loc = storage_loc

            self.write_config_settings()

        # continually try and create the connection until successful
        while True:
            try:
                self.socket = socket.create_connection((self.address, self.port))
                break
            except socket.error:
                time.sleep(util.slave_connect_wait)

        # initial connection protocol
        self.socket.sendall(util.i_to_bytes(self.id))
        self.id = util.i_from_bytes(self.socket.recv(util.bufsize))
        self.socket.sendall(util.i_to_bytes(self.storage_space))

        self.write_config_settings()

    def read_config_settings(self):
        if not os.path.isfile(util.config_file):
            raise FileExistsError()
        file = open(util.config_file, "r")

        self.address = file.readline().replace("\n", "")
        self.port = int(file.readline().replace("\n", ""))
        self.storage_space = int(file.readline().replace("\n", ""))
        self.storage_loc = file.readline().replace("\n", "")
        self.id = int(file.readline().replace("\n", ""))

        file.close()

    def write_config_settings(self):
        file = open(util.config_file, "w")

        file.write(self.address + "\n")
        file.write(str(self.port) + "\n")
        file.write(str(self.storage_space) + "\n")
        file.write(self.storage_loc + "\n")
        file.write(str(self.id) + "\n")

        file.close()

    def start(self):
        while True:
            command = util.s_from_bytes(self.socket.recv(util.bufsize))

            if command == "OPEN":
                self.socket.sendall(util.s_to_bytes("OPEN"))
            else:
                print("unrecognized command")


def main(args):
    # set up the master controller
    if len(args) > 1:
        slave = Slave(args[1], int(args[2]), int(args[3]), args[4])
    else:
        slave = Slave()

    slave.start()

    return


if __name__ == "__main__":
    main(sys.argv)
