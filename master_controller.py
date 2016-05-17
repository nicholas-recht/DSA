import sys
import socket
import threading
import sqlite3
import os
import util
import time
import select


class SlaveNode:
    def __init__(self):
        self.socket = None
        self.address = None
        self.id = -1
        self.storage_space = 0
        self.status = "inactive"


class WelcomeSocket:
    def __init__(self, new_client_callback=None, port=15719):
        self.welcome_socket = None
        self.callback = new_client_callback
        self.port = port

    def open_welcome_conn(self):
        # create an INET, STREAMing socket
        self.welcome_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # bind the socket to a public host, and a well-known port
        self.welcome_socket.bind((socket.gethostname(), self.port))
        # become a server socket
        self.welcome_socket.listen(5)

        print("Welcoming socket: " + socket.gethostname(), " ", str(self.port))

        while True:
            # accept connections from outside
            (client_socket, address) = self.welcome_socket.accept()

            client = SlaveNode()
            client.address = address
            client.socket = client_socket

            print("New connection created")

            self.callback(client)

        self.welcome_socket.close()


class Master:
    def __init__(self):
        # member variables
        self.nodes = []
        self.ready = False
        self.command_socket = None
        self.execute = True

        # locks
        self.nodes_lock = threading.Lock()

        # check if an existing database already exists
        if not os.path.isfile(util.database):
            print("First time setup")
            self.setup_db()

        else:
            self.nodes = self.get_slave_nodes()

        # set up the welcoming socket for new threads
        print("Create welcome thread")
        welcome_activity = WelcomeSocket(new_client_callback=self.accept_new_node)
        welcome_thread = threading.Thread(target=welcome_activity.open_welcome_conn, daemon=True)
        welcome_thread.start()

        # start the synchronization period for already connected nodes
        print("Start synchronization period")
        wait_time = util.restart_window
        while len([x for x in self.nodes if x.status == "restart"]) > 0 and wait_time > 0:
            wait_time -= util.wait_interval
            time.sleep(util.wait_interval)
        print("End synchronization period")

        # any nodes that haven't reconnected are now lost
        lost_nodes = [x for x in self.nodes if x.status == "restart"]
        for node in lost_nodes:
            self.lose_node(node)

        # any new nodes are now connected
        new_nodes = [x for x in self.nodes if x.status == "new"]
        for node in new_nodes:
            node.status = "connected"
            self.update_slave_node(node)

        # set up command socket
        # create an INET, STREAMing socket
        print("Create command socket")
        self.command_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # bind the socket to a public host, and a well-known port
        self.command_socket.bind(("localhost", util.command_port))
        # become a server socket
        self.command_socket.listen(5)

        # set up the continuous connection thread
        print("Create continuous connection thread")
        con_thread = threading.Thread(target=self.continuous_connection, daemon=True)
        con_thread.start()

        # ready to begin normal execution
        self.ready = True

    def continuous_connection(self):
        while self.execute:
            print("Nodes connected: ", str(len(self.nodes)))
            threads = []
            for node in self.nodes:
                t = threading.Thread(target=self.check_connection, args=(node,))
                threads.append(t)
                t.start()
            for t in threads:
                t.join()
            time.sleep(util.master_continuous_wait)

    def check_connection(self, node):
        try:
            # send connect command
            node.socket.sendall(util.s_to_bytes("OPEN"))
            ready = select.select([node.socket], [], [], .8)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response != "OPEN":
                raise socket.error("invalid response")
        except socket.error:
            self.lose_node(node)

    def lose_node(self, node):
        node.status = "lost"

        # remove the node from the array
        self.nodes_lock.acquire()
        self.nodes.remove(node)
        self.nodes_lock.release()

        # start recovery stuff here
        self.update_slave_node(node)
        return

    def accept_new_node(self, node):
        # kick off a new thread to handle the initial handshake
        handshake_thread = threading.Thread(target=self.handshake_node, args=(node,))
        handshake_thread.start()

    def handshake_node(self, node):
        """
        This function should be run in it's own thread.
        :param node:
        :return:
        """
        if self.ready:
            node.status = "connected"
        else:
            node.status = "new"

        node.id = util.i_from_bytes(node.socket.recv(util.bufsize))

        # if id == -1 then it's a new node
        if node.id == -1:
            self.insert_slave_node(node)
            print("New node connected")
        else:
            # check the case where connecting node doesn't give a valid id
            existing_node = self.get_slave_node(node.id)
            if existing_node is None:
                # treat it like a new node
                self.insert_slave_node(node)
                print("Invalid node connected")
            else:
                print("Existing node connected")

        # send id to the node
        node.socket.sendall(util.i_to_bytes(node.id))
        # get the storage space of the new node
        node.storage_space = util.i_from_bytes(node.socket.recv(util.bufsize))

        self.update_slave_node(node)
        # add the node to the list of nodes
        # LOCK
        self.nodes_lock.acquire()
        # CRITICAL SECTION
        self.nodes.append(node)
        # UNLOCK
        self.nodes_lock.release()

    def setup_db(self):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()
        # Create the tables
        c.execute('''CREATE TABLE tbl_folder
                    (id INTEGER NOT NULL PRIMARY KEY,
                     parent_id INTEGER NOT NULL,
                     name TEXT NOT NULL,
                     FOREIGN KEY(parent_id) REFERENCES tbl_folder(id))''')

        c.execute('''CREATE TABLE tbl_file
                      (id INTEGER NOT NULL PRIMARY KEY,
                       name TEXT NOT NULL,
                       size BIGINT NOT NULL,
                       upload_date TEXT NOT NULL,
                       folder_id INTEGER NOT NULL,
                       FOREIGN KEY(folder_id) REFERENCES tbl_folder(id))''')

        c.execute('''CREATE TABLE tbl_slave_node
                      (id INTEGER NOT NULL PRIMARY KEY,
                       storage_space BIGINT NOT NULL,
                       status TEXT)''')

        c.execute(('''CREATE TABLE tbl_file_part
                      (id INTEGER NOT NULL PRIMARY KEY,
                       file_id INTEGER NOT NULL,
                       node_id INTEGER NOT NULL,
                       access_name TEXT NOT NULL,
                       sequence_order INTEGER,
                       FOREIGN KEY(file_id) REFERENCES tbl_file(id) ON UPDATE CASCADE,
                       FOREIGN KEY(node_id) REFERENCES tbl_slave_node(id))'''))

        c.execute('''CREATE UNIQUE INDEX ux_tbl_file_part ON tbl_file_part(file_id, sequence_order)''')

        # end
        conn.commit()
        conn.close()

    def get_slave_nodes(self):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_slave_node WHERE status = 'restart'")

        rows = c.fetchall()
        nodes = []
        for row in rows:
            node = SlaveNode()
            node.id = row[0]
            node.storage_space = row[1]
            node.status = row[2]
            nodes.append(node)

        conn.close()

        return nodes

    def get_slave_node(self, id):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (id,)
        c.execute('SELECT * FROM tbl_slave_node WHERE id = ?', params)

        row = c.fetchone()
        node = None
        if row is not None:
            node = SlaveNode()
            node.id = row[0]
            node.storage_space = row[1]
            node.status = row[2]

        conn.close()

        return node

    def update_slave_node(self, node):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (node.status, node.storage_space, node.id)

        c.execute('''UPDATE tbl_slave_node SET
                     status = ?,
                     storage_space = ?
                     WHERE id = ?''', params)
        conn.commit()
        conn.close()

    def insert_slave_node(self, node):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (node.status, node.storage_space)

        c.execute('''INSERT INTO tbl_slave_node
                     (status,
                      storage_space)
                     VALUES
                     (?,
                      ?)''', params)
        node.id = c.lastrowid
        conn.commit()
        conn.close()

    def clear_db(self):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute('''
                    DELETE FROM tbl_slave_node
                    ''')

        conn.commit()
        conn.close()

    def close(self):
        for node in self.nodes:
            node.status = "restart"
            node.socket.sendall(util.s_to_bytes("CLOSE"))
            self.update_slave_node(node)
        self.execute = False

    def start(self):
        # main process loop - listen for commands
        while True:
            # accept connections from outside
            (client_socket, address) = self.command_socket.accept()
            command = util.s_from_bytes(client_socket.recv(util.bufsize))

            if command == "test":
                print("test command received")
            elif command == "clear_database":
                self.clear_db()
                print("database cleared")
            elif command == "show_nodes":
                print(self.nodes)
            elif command == "close":
                self.close()
                print("Closing master controller")
                return
            else:
                print("Unrecognized command")

            client_socket.close()

        self.command_socket.close()


def main(args):
    # set up the master controller
    master = Master()
    master.start()
    return


if __name__ == "__main__":
    main(sys.argv)
