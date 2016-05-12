import sys
import socket
import threading
import sqlite3
import os
import util
import time


# config settings
database = "database.db"
bufsize = 1024
restart_window = 3.0
wait_interval = .1
# status: new -> connected -> restart -> lost

class SlaveNode:
    def __init__(self):
        self.socket = None
        self.address = None
        self.id = -1
        self.storage_space = 0
        self.status = "inactive"


class WelcomeSocket:
    def __init__(self, new_client_callback=None):
        self.welcome_socket = None
        self.callback = new_client_callback

    def open_welcome_conn(self):
        # create an INET, STREAMing socket
        self.welcome_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # bind the socket to a public host, and a well-known port
        self.welcome_socket.bind((socket.gethostname(), 15670))
        # become a server socket
        self.welcome_socket.listen(5)

        while True:
            # accept connections from outside
            (client_socket, address) = self.welcome_socket.accept()

            client = SlaveNode()
            client.address = address
            client.socket = client_socket

            self.callback(client)

        self.welcome_socket.close()


class Master:
    def __init__(self):
        # member variables
        self.nodes = []
        self.ready = False

        # locks
        self.nodes_lock = threading.Lock()

        # check if an existing database already exists
        if not os.path.isfile(database):
            print("First time setup")
            self.setup_db()

        else:
            self.nodes = self.get_slave_nodes()

        # set up the welcoming socket for new threads
        welcome_activity = WelcomeSocket(new_client_callback=self.accept_new_node)
        welcome_thread = threading.Thread(target=welcome_activity.open_welcome_conn)
        welcome_thread.start()

        # start the synchronization period for already connected nodes
        wait_time = restart_window
        if len(self.nodes) > 0:
            initial_length = len(self.nodes)
            while len([x for x in self.nodes if x.status == "connected"]) < initial_length and wait_time > 0:
                wait_time -= wait_interval
                time.sleep(wait_interval)

        # any nodes that haven't reconnected are now lost
        lost_nodes = [x for x in self.nodes if x.status == "restart"]
        for node in lost_nodes:
            self.lose_node(node)
        # any new nodes are now connected
        lost_nodes = [x for x in self.nodes if x.status == "new"]
        for node in lost_nodes:
            node.status = "connected"
            self.update_slave_node(node)

        # ready to begin normal execution
        self.ready = True

    def lose_node(self, node):
        node.status = "lost"

        # start recovery stuff here
        self.update_slave_node(node)
        return

    def accept_new_node(self, node):
        # kick off a new thread to handle the initial handshake
        handshake_thread = threading.Thread(target=self.handshake_node, args=node)
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

        node.id = util.i_from_bytes(node.socket.recv(bufsize))

        # if id == -1 then it's a new node
        if node.id == -1:
            self.insert_slave_node(node)
            # send new id to the node
            node.socket.sendall(util.i_to_bytes(node.id))
            # get the storage space of the new node
            node.storage_space = util.i_from_bytes(node.socket.recv(bufsize))

            self.update_slave_node(node)
            # add the node to the list of nodes
            # LOCK
            self.nodes_lock.acquire()
            # CRITICAL SECTION
            self.nodes.append(node)
            # UNLOCK
            self.nodes_lock.release()
        else:
            # check the case where connecting node doesn't give a valid id
            existing_node = self.get_slave_node(node.id)
            # TODO

    def setup_db(self):
        # open the connection
        conn = sqlite3.connect(database)
        c = conn.cursor()
        # Create the tables
        c.execute('''CREATE TABLE tbl_file
                      (id INTEGER NOT NULL PRIMARY KEY,
                       name TEXT NOT NULL,
                       size BIGINT NOT NULL,
                       upload_date TEXT NOT NULL)''')

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
        conn = sqlite3.connect(database)
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
        conn = sqlite3.connect(database)
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
        conn = sqlite3.connect(database)
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
        conn = sqlite3.connect(database)
        c = conn.cursor()

        params = (node.status, node.storage_space)

        c.execute('''INSERT INTO tbl_slave_node
                     (status,
                      storage_space)
                     VALUES
                     (status = ?,
                      storage_space = ?)''', params)
        node.id = c.lastrowid
        conn.commit()
        conn.close()

def main(args):
    # set up the master controller
    master = Master()
    return


if __name__ == "__main__":
    main(sys.argv)
