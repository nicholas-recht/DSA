import os
import sys
import socket
import time
if __name__ == "__main__":
    import util
else:
    from . import util


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
                self.socket = socket.create_connection((self.address, self.port), util.slave_connect_timeout)
                self.socket.settimeout(None)
                break
            except socket.error as e:
                print(str(e))
                time.sleep(util.slave_connect_wait)
        print("Connection established")

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

    def upload_file(self):
        file_name = util.s_from_bytes(self.socket.recv(util.bufsize))
        self.socket.sendall(util.s_to_bytes("OK"))
        file_size = util.i_from_bytes(self.socket.recv(util.bufsize))
        self.socket.sendall(util.s_to_bytes("OK"))

        # get the file in "chunks" of bufsize
        bytes = bytearray()
        num_got = 0
        while num_got < file_size:
            chunk = self.socket.recv(util.bufsize)
            bytes += chunk
            num_got += len(chunk)

        self.socket.sendall(util.s_to_bytes("OK"))

        # write the file
        file = open(self.storage_loc + '/' + file_name, mode='wb')
        file.write(bytes)
        file.close()

        print("File uploaded")

    def delete_file(self):
        file_name = util.s_from_bytes(self.socket.recv(util.bufsize))

        os.remove(self.storage_loc + '/' + file_name)

        self.socket.sendall(util.s_to_bytes("OK"))

        print("File deleted")

    def download_file(self):
        file_name = util.s_from_bytes(self.socket.recv(util.bufsize))

        try:
            file = open(self.storage_loc + '/' + file_name, mode='rb')
            f = file.read()
            bytes = bytearray(f)

            self.socket.sendall(util.i_to_bytes(len(bytes)))

            response = util.s_from_bytes(self.socket.recv(util.bufsize))
            if response != "SEND":
                raise Exception("Unrecognized command")

            self.socket.sendall(bytes)

            file.close()

            print("File downloaded")

        except FileNotFoundError as e:
            self.socket.sendall(util.s_to_bytes("FAIL"))
            print(str(e))

    def file_contains_substring(self, path, substr):
        try:
            file = open(self.storage_loc + '/' + path, mode='rb')
            f = file.read()

            if f.find(substr) != -1:
                return True
            else:
                return False

        except Exception as e:
            return False

    def search_files(self):
        search_string = self.socket.recv(util.bufsize)  # we want to use bytes because the file will be read as bytes

        files = os.listdir(self.storage_loc)

        matching = [file for file in files if (self.file_contains_substring(file, search_string))]

        if len(matching) > 0:
            self.socket.sendall(util.s_to_bytes(','.join(matching)))
        else:
            self.socket.sendall(util.s_to_bytes("NONE"))

    def start(self):
        while True:
            command = util.s_from_bytes(self.socket.recv(util.bufsize))

            if command == "OPEN":
                self.socket.sendall(util.s_to_bytes("OPEN"))
            elif command == "CLOSE":
                print("Close command received")
                break
            elif command == "UPLOAD":
                self.socket.sendall(util.s_to_bytes("OK"))
                self.upload_file()
            elif command == "DOWNLOAD":
                self.socket.sendall(util.s_to_bytes("OK"))
                self.download_file()
            elif command == "DELETE":
                self.socket.sendall(util.s_to_bytes("OK"))
                self.delete_file()
            elif command == "SEARCH":
                self.socket.sendall(util.s_to_bytes("OK"))
                self.search_files()
            else:
                self.socket.sendall(util.s_to_bytes("UNKNOWN"))
                print("unrecognized command")

        # close the open socket
        self.socket.close()


def main(args):
    try:
        # set up the master controller
        if len(args) > 1:
            slave = Slave(args[1], int(args[2]), int(args[3]), args[4])
        else:
            slave = Slave()

        slave.start()
    except Exception as e:
        print(str(e))
        # start over again if there is a failure
        main([])

    return


if __name__ == "__main__":
    main(sys.argv)
