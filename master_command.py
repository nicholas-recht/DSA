import sys
import socket
import util


def main(args):
    if len(args) < 2:
        print("usage: master_command <command>")
        print("available commands: \n\ttest\n\tclear_database\n\tshow_nodes")
        sys.exit()

    command = args[1]

    s = socket.create_connection(("localhost", util.command_port))
    s.sendall(util.s_to_bytes(command))
    s.close()


if __name__ == "__main__":
    main(sys.argv)