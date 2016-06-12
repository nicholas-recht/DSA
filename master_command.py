import sys
import socket
if __name__ == "__main__":
    import util
else:
    from . import util


def main(args):
    if len(args) == 2:
        command = args[1]

        s = socket.create_connection(("localhost", util.command_port))
        s.sendall(util.s_to_bytes(command))
        s.close()
    elif len(args) == 3:
        command = args[1]
        param = args[2]
        s = socket.create_connection(("localhost", util.command_port))
        s.sendall(util.s_to_bytes(command))
        res = s.recv(util.bufsize)
        s.sendall(util.s_to_bytes(param))
        s.close()

    else:
        print("usage: master_command <command>")
        print("available commands: \n\ttest\n\tclear_database\n\tshow_nodes")
        sys.exit()

if __name__ == "__main__":
    main(sys.argv)