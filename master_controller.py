import sys
import socket
import threading
import sqlite3
import os
import time
import select
import datetime
import math
if __name__ == "__main__":
    import util
else:
    from . import util


class SlaveNode:
    def __init__(self):
        self.socket = None
        self.address = None
        self.id = -1
        self.storage_space = 0
        self.status = "inactive"

    @staticmethod
    def get_slave_nodes():
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

    @staticmethod
    def get_slave_node(id):
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

    @staticmethod
    def update_slave_node(node):
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

    @staticmethod
    def insert_slave_node(node):
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

    @staticmethod
    def clear_db():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute('''
                    DELETE FROM tbl_slave_node
                    ''')

        conn.commit()
        conn.close()


class File:
    def __init__(self):
        self.id = -1
        self.name = ""
        self.size = 0
        self.upload_date = None
        self.folder_id = 1

    def to_string(self):
        return str(self.id) + " " + self.name + " " + util.datetime_to_s(self.upload_date)

    @staticmethod
    def get_files():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_file")

        rows = c.fetchall()
        files = []
        for row in rows:
            file = File()
            file.id = row[0]
            file.name = row[1]
            file.size = row[2]
            file.upload_date = util.datetime_from_s(row[3])
            file.folder_id = row[4]
            files.append(file)

        conn.close()

        return files

    @staticmethod
    def get_file(id):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (id,)
        c.execute("SELECT * FROM tbl_file WHERE id = ?", params)

        row = c.fetchone()
        file = None
        if row is not None:
            file = File()
            file.id = row[0]
            file.name = row[1]
            file.size = row[2]
            file.upload_date = util.datetime_from_s(row[3])
            file.folder_id = row[4]

        conn.close()

        return file

    @staticmethod
    def insert_file(file):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (file.name, file.size, util.datetime_to_s(file.upload_date), file.folder_id)

        c.execute('''INSERT INTO tbl_file
                         (  name,
                            size,
                            upload_date,
                            folder_id)
                         VALUES
                         (  ?,
                            ?,
                            ?,
                            ?)''', params)
        file.id = c.lastrowid
        conn.commit()
        conn.close()

    @staticmethod
    def update_file(file):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (file.name, file.size, util.datetime_to_s(file.upload_date), file.folder_id, file.id)

        c.execute('''UPDATE tbl_file SET
                        name = ?,
                        size = ?,
                        upload_date = ?,
                        folder_id = ?
                    WHERE id = ?''', params)

        conn.commit()
        conn.close()

    @staticmethod
    def delete_file(file):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (file.id,)

        c.execute('''DELETE FROM tbl_file WHERE id = ?
                            ''', params)

        conn.commit()
        conn.close()

    @staticmethod
    def clear_db():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute('''DELETE FROM tbl_file
                        ''')

        conn.commit()
        conn.close()


class Folder:
    def __init__(self):
        self.id = -1
        self.parent_id = -1
        self.name = ''
        self.children = {}

    @staticmethod
    def get_folders():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_folder")

        rows = c.fetchall()
        folders = []
        for row in rows:
            folder = Folder()
            folder.id = row[0]
            folder.parent_id = row[1]
            folder.name = row[2]

            folders.append(folder)

        conn.close()

        return folders

    @staticmethod
    def get_folder(id):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (id,)
        c.execute("SELECT * FROM tbl_folder WHERE id = ?", params)

        row = c.fetchone()
        folder = None
        if row is not None:
            folder = Folder()
            folder.id = row[0]
            folder.parent_id = row[1]
            folder.name = row[2]

        conn.close()

        return folder

    @staticmethod
    def insert_folder(folder):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (folder.parent_id, folder.name)

        c.execute('''INSERT INTO tbl_folder
                             (  parent_id,
                                name)
                             VALUES
                             (  ?,
                                ?)''', params)

        folder.id = c.lastrowid
        conn.commit()
        conn.close()

    @staticmethod
    def update_folder_name(folder):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (folder.name, folder.id)

        c.execute('''UPDATE tbl_folder SET
                            name = ?
                     WHERE id = ?''', params)

        conn.commit()
        conn.close()

    @staticmethod
    def update_folder_parent(folder, parent):
        # make sure the given parent isn't actually a child of the folder
        if Folder.is_parent(parent, folder):
            raise Exception("Cannot move a folder to its own subdirectory")

        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (parent.id, folder.id)

        c.execute('''UPDATE tbl_folder SET
                            parent_id = ?
                     WHERE id = ?''', params)

        conn.commit()
        conn.close()

        folder.parent_id = parent.id

    @staticmethod
    def delete_folder(folder):
        files = [file for file in File.get_files() if file.folder_id == folder.id]
        if len(files) > 0:
            raise Exception("The folder is not empty.")

        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (folder.id,)

        c.execute('''DELETE FROM tbl_folder WHERE id = ?''', params)

        conn.commit()
        conn.close()

    @staticmethod
    def get_folder_map():
        folders = Folder.get_folders()

        map = {}
        for folder in folders:
            map[folder.id] = folder

        return map

    @staticmethod
    def get_folder_hierarchy():
        folder_map = Folder.get_folder_map()

        root = folder_map[1]
        if root.name != "root" and root.id != 1 and root.parent_id != 1:
            raise Exception("No root directory found")

        for key, val in folder_map.items():
            if key != 1:
                folder_map[val.parent_id].children[val.name] = val

        return root

    @staticmethod
    def is_parent(folder, parent):
        """
        Returns whether parent is a parent directory of the given folder
        :param folder:
        :param parent:
        :return:
        """
        map = Folder.get_folder_map()
        tmp = folder

        while tmp.id != 1:
            if parent.id == tmp.id:
                return True

            tmp = map[tmp.parent_id]

        return False

    @staticmethod
    def clear_db():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute('''DELETE FROM tbl_folder WHERE id != 1''')

        conn.commit()
        conn.close()


class FilePart:
    def __init__(self):
        self.id = -1
        self.file_id = -1
        self.node_id = -1
        self.access_name = ""
        self.sequence_order = -1
        self.size = 0

    @staticmethod
    def get_file_parts():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_file_part")

        rows = c.fetchall()
        file_parts = []
        for row in rows:
            part = FilePart()
            part.id = row[0]
            part.file_id = row[1]
            part.node_id = row[2]
            part.access_name = row[3]
            part.sequence_order = row[4]
            part.size = row[5]
            file_parts.append(part)

        conn.close()

        return file_parts

    @staticmethod
    def get_lost_file_parts():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_file_part_lost")

        rows = c.fetchall()
        file_parts = []
        for row in rows:
            part = FilePart()
            part.id = row[0]
            part.file_id = row[1]
            part.node_id = row[2]
            part.access_name = row[3]
            part.sequence_order = row[4]
            part.size = row[5]
            file_parts.append(part)

        conn.close()

        return file_parts

    @staticmethod
    def get_file_part(id):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (id,)
        c.execute("SELECT * FROM tbl_file_part WHERE id = ?", params)

        row = c.fetchone()
        part = None
        if row is not None:
            part = FilePart()
            part.id = row[0]
            part.file_id = row[1]
            part.node_id = row[2]
            part.access_name = row[3]
            part.sequence_order = row[4]
            part.size = row[5]

        conn.close()

        return part

    @staticmethod
    def get_lost_file_part(id):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (id,)
        c.execute("SELECT * FROM tbl_file_part_lost WHERE id = ?", params)

        row = c.fetchone()
        part = None
        if row is not None:
            part = FilePart()
            part.id = row[0]
            part.file_id = row[1]
            part.node_id = row[2]
            part.access_name = row[3]
            part.sequence_order = row[4]
            part.size = row[5]

        conn.close()

        return part

    @staticmethod
    def insert_file_part(part):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (part.file_id, part.node_id, part.access_name, part.sequence_order, part.size)

        c.execute('''INSERT INTO tbl_file_part
                             (  file_id,
                                node_id,
                                access_name,
                                sequence_order,
                                size)
                             VALUES
                             (  ?,
                                ?,
                                ?,
                                ?,
                                ?)''', params)
        part.id = c.lastrowid
        conn.commit()
        conn.close()

    @staticmethod
    def insert_lost_file_part(part):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (part.id, part.file_id, part.node_id, part.access_name, part.sequence_order, part.size)

        c.execute('''INSERT INTO tbl_file_part_lost
                                 (  id,
                                    file_id,
                                    node_id,
                                    access_name,
                                    sequence_order,
                                    size)
                                 VALUES
                                 (  ?,
                                    ?,
                                    ?,
                                    ?,
                                    ?,
                                    ?)''', params)

        conn.commit()
        conn.close()

    @staticmethod
    def update_file_part(part):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (part.file_id, part.node_id, part.access_name, part.sequence_order, part.size, part.id)

        c.execute('''UPDATE tbl_file_part SET
                            file_id = ?,
                            node_id = ?,
                            access_name = ?,
                            sequence_order = ?,
                            size = ?
                     WHERE id = ?''', params)

        conn.commit()
        conn.close()

    @staticmethod
    def delete_file_part(part):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (part.id,)

        c.execute('''DELETE FROM tbl_file_part WHERE id = ?
                                ''', params)

        conn.commit()
        conn.close()

    @staticmethod
    def delete_lost_file_part(part):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (part.id,)

        c.execute('''DELETE FROM tbl_file_part_lost WHERE id = ?
                                    ''', params)

        conn.commit()
        conn.close()

    @staticmethod
    def clear_db():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute('''DELETE FROM tbl_file_part
                            ''')

        c.execute('''DELETE FROM tbl_file_part_lost
            ''')

        conn.commit()
        conn.close()


class WelcomeSocket:
    def __init__(self, new_client_callback=None, port=15719):
        self.welcome_socket = None
        self.callback = new_client_callback
        self.port = port

    def open_welcome_conn(self):
        # create an INET, STREAMing socket
        self.welcome_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # bind the socket to a public host, and a well-known port
        self.welcome_socket.bind(("", self.port))
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
        self.busy = False
        self.command_thread = None

        # locks
        self.nodes_lock = threading.Lock()

        # check if an existing database already exists
        if not os.path.isfile(util.database):
            print("First time setup")
            self.setup_db()

        else:
            self.nodes = SlaveNode.get_slave_nodes()

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
            SlaveNode.update_slave_node(node)

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
            if not self.busy:
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
            ready = select.select([node.socket], [], [], util.master_continuous_wait - .2)
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
        SlaveNode.update_slave_node(node)
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
            SlaveNode.insert_slave_node(node)
            print("New node connected")
        else:
            # check the case where connecting node doesn't give a valid id
            existing_node = SlaveNode.get_slave_node(node.id)
            if existing_node is None:
                # treat it like a new node
                SlaveNode.insert_slave_node(node)
                print("Invalid node connected")
            else:
                print("Existing node connected")

        # send id to the node
        node.socket.sendall(util.i_to_bytes(node.id))
        # get the storage space of the new node
        node.storage_space = util.i_from_bytes(node.socket.recv(util.bufsize))

        SlaveNode.update_slave_node(node)
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
                       sequence_order INTEGER NOT NULL,
                       size INTEGER NOT NULL,
                       FOREIGN KEY(file_id) REFERENCES tbl_file(id) ON UPDATE CASCADE,
                       FOREIGN KEY(node_id) REFERENCES tbl_slave_node(id))'''))

        c.execute(('''CREATE TABLE tbl_file_part_lost
            (id INTEGER NOT NULL PRIMARY KEY,
             file_id INTEGER NOT NULL,
             node_id INTEGER NOT NULL,
             access_name TEXT NOT NULL,
             sequence_order INTEGER NOT NULL,
             size INTEGER NOT NULL,
             FOREIGN KEY(node_id) REFERENCES tbl_slave_node(id))'''))

        c.execute('''CREATE UNIQUE INDEX ux_tbl_file_part ON tbl_file_part(file_id, sequence_order)''')

        c.execute('''CREATE UNIQUE INDEX ux_tbl_folder ON tbl_folder(parent_id, name)''')

        # end
        conn.commit()
        conn.close()

        # add the root directory
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (1, 1, "root")
        c.execute('''INSERT INTO tbl_folder
                    (  id,
                       parent_id,
                       name)
                    VALUES
                    (  ?,
                       ?,
                       ?)''', params)

        conn.commit()
        conn.close()

    def close(self):
        for node in self.nodes:
            node.status = "restart"
            node.socket.sendall(util.s_to_bytes("CLOSE"))
            SlaveNode.update_slave_node(node)
        self.execute = False

    def upload_file(self, name, bytes, folder_id=1):
        # check if there is enough space
        if len(bytes) > self.get_total_space_available():
            raise Exception("Not enough space available")

        # TODO check each node individually and make sure it has enough space

        # create the new file object
        file_obj = File()
        file_obj.folder_id = folder_id
        file_obj.upload_date = datetime.datetime.utcnow()
        file_obj.name = name
        file_obj.size = len(bytes)

        File.insert_file(file_obj)

        # split the file into parts
        num_splits = len(self.nodes)
        if num_splits > 0:
            split_size = math.ceil(file_obj.size / num_splits)
            index = 0
            array_index = 0
            parts = [None] * num_splits
            part_bytes = [None] * num_splits
            while index < num_splits:
                part_bytes[index] = bytes[array_index:array_index + split_size]

                part = FilePart()
                part.node_id = self.nodes[index].id
                part.file_id = file_obj.id
                part.sequence_order = index
                part.access_name = str(file_obj.id) + "_" + str(index)
                part.size = len(part_bytes[index])

                parts[index] = part

                index += 1
                array_index += split_size

            # send each part to the slave nodes
            self.busy = True
            msgs = [None] * num_splits
            threads = [None] * num_splits
            index = 0
            while index < num_splits:
                t = threading.Thread(target=self.upload_part,
                                     args=(self.nodes[index],
                                           parts[index].access_name,
                                           part_bytes[index],
                                           msgs,
                                           index))
                threads[index] = t
                t.start()
                index += 1
            for t in threads:
                t.join()

            self.busy = False
            # check for any errors
            errors = [x for x in msgs if x is not None]
            if len(errors) > 0:
                print("Error sending file")
                for error in errors:
                    print(error)
                # TODO delete any parts that were successfully sent
                # TODO delete the file obj from the database
            else:
                for part in parts:
                    FilePart.insert_file_part(part)

                # return the file_obj if everything was successfully completed
                return file_obj

        else:
            print("Error: no connected nodes")

        return None

    def upload_part(self, node, name, bytes, errors, index):
        try:
            # send command
            node.socket.sendall(util.s_to_bytes("UPLOAD"))
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response != "OK":
                raise socket.error("upload time out")
            # send the name
            node.socket.sendall(util.s_to_bytes(name))
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response != "OK":
                raise socket.error("upload time out")
            # send the size of the file part
            node.socket.sendall(util.i_to_bytes(len(bytes)))
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response != "OK":
                raise socket.error("upload time out")
            # send the file part
            node.socket.sendall(bytes)
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response != "OK":
                raise socket.error("upload time out")

        except socket.error as e:
            errors[index] = str(e)

    def download_file(self, id):
        try:
            file_obj = File.get_file(id)
            if file_obj is None:
                raise Exception("The requested file does not exist")

            # get all the file parts
            file_parts = [x for x in FilePart.get_file_parts() if x.file_id == file_obj.id]
            num_parts = max(file_parts,
                            key=lambda var: var.sequence_order)\
                .sequence_order + 1  # the number of "unique" file parts

            node_dict = {}  # key: node_id  value: node
            for node in self.nodes:
                node_dict[node.id] = node

            part_found = [False] * num_parts  # has the part with the given sequence_order been found?
            part_dict = {}  # key: part_sequence_order  value: part
            part_on_node_dict = {}  # key: part_sequence_order  value: node that stores it
            for part in file_parts:
                if part.node_id in node_dict:
                    part_found[part.sequence_order] = True
                    if part.sequence_order not in part_on_node_dict:
                        part_on_node_dict[part.sequence_order] = node_dict[part.node_id]
                        part_dict[part.sequence_order] = part

            # check if all file parts are accounted for
            if len([x for x in part_found if x is False]) > 0:
                raise Exception("Not all file parts are available from the set of nodes currently connected")

            # get the part from each node
            self.busy = True
            rtnVal = [None] * num_parts
            threads = [None] * num_parts
            index = 0
            while index < num_parts:
                t = threading.Thread(target=self.download_part,
                                     args=(part_on_node_dict[index],
                                           part_dict[index].access_name,
                                           rtnVal,
                                           index))
                threads[index] = t
                t.start()
                index += 1
            for t in threads:
                t.join()

            self.busy = False
            # check for any errors
            part_contents = [x for x in rtnVal if isinstance(x, bytearray)]
            if len(part_contents) < num_parts:
                print("Error receiving file")
                errors = [x for x in rtnVal if isinstance(x, str)]
                for error in errors:
                    print(error)
                    # TODO handle error better
            else:
                file_contents = bytearray()
                for part_content in part_contents:
                    file_contents += part_content

                return file_contents

        except Exception as e:
            print(str(e))

            return None

    def download_part(self, node, name, rtnVal, index):
        try:
            # send command
            node.socket.sendall(util.s_to_bytes("DOWNLOAD"))
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response != "OK":
                raise socket.error("download time out")
            # send the name
            node.socket.sendall(util.s_to_bytes(name))
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.i_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response == "FAIL":
                raise socket.error("upload time out")
            file_size = response
            # send the size of the file part
            node.socket.sendall(util.s_to_bytes("SEND"))

            # get the file in "chunks" of bufsize
            bytes = bytearray()
            num_got = 0
            while num_got < file_size:
                chunk = node.socket.recv(util.bufsize)
                bytes += chunk
                num_got += len(chunk)

            rtnVal[index] = bytes

        except socket.error as e:
            rtnVal[index] = str(e)

    def delete_file(self, id):
        try:
            file_obj = File.get_file(id)
            if file_obj is None:
                raise Exception("The requested file does not exist")

            # get all the file parts
            file_parts = [x for x in FilePart.get_file_parts() if x.file_id == file_obj.id]
            num_parts = len(file_parts)

            node_dict = {}  # key: node_id  value: node
            for node in self.nodes:
                node_dict[node.id] = node

            # delete each file part where the node is connected
            self.busy = True
            rtnVal = []
            threads = []
            parts = []
            index = 0
            for part in file_parts:
                if part.node_id in node_dict:
                    rtnVal.append(None)
                    parts.append(part)
                    t = threading.Thread(target=self.delete_part,
                                         args=(node_dict[part.node_id],
                                               part.access_name,
                                               rtnVal,
                                               index))
                    threads.append(t)
                    t.start()
                else:
                    FilePart.delete_file_part(part)
                    FilePart.insert_lost_file_part(part)
                index += 1

            for t in threads:
                t.join()
            self.busy = False

            # check for any errors
            for idx, val in enumerate(rtnVal):
                part = parts[idx]
                FilePart.delete_file_part(part)

                if isinstance(val, str):
                    print(val)
                    FilePart.insert_lost_file_part(part)

            File.delete_file(file_obj)

        except Exception as e:
            print(str(e))

            return None

    def delete_part(self, node, name, errors, index):
        try:
            # send command
            node.socket.sendall(util.s_to_bytes("DELETE"))
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response != "OK":
                raise socket.error("upload time out")
            # send the name
            node.socket.sendall(util.s_to_bytes(name))
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response != "OK":
                raise socket.error("upload time out")

        except socket.error as e:
            errors[index] = str(e)

    def listen_for_commands(self):
        if self.command_thread is None:
            self.command_thread = threading.Thread(target=self.command_loop, daemon=True)
            self.command_thread.start()

    def command_loop(self):
        # main process loop - listen for commands
        while True:
            try:
                # accept connections from outside
                (client_socket, address) = self.command_socket.accept()
                command = util.s_from_bytes(client_socket.recv(util.bufsize))

                if command == "test":
                    print("test command received")
                elif command == "clear_database":
                    SlaveNode.clear_db()
                    File.clear_db()
                    FilePart.clear_db()
                    Folder.clear_db()
                    print("database cleared")
                elif command == "show_nodes":
                    print(self.nodes)
                elif command == "space_available":
                    print(str(self.get_total_space_available()))
                elif command == "total_space":
                    print(str(self.get_total_size()))
                elif command == "show_files":
                    files = File.get_files()
                    for file in files:
                        print(file.to_string())
                elif command == "close":
                    self.close()
                    print("Closing master controller")
                    return
                elif command == "upload":
                    print("Upload command received")
                    client_socket.sendall(util.s_to_bytes("OK"))
                    file_path = util.s_from_bytes(client_socket.recv(util.bufsize))

                    file = open(file_path, mode='rb')
                    name = file.name.replace("\\", "/")
                    name = name.split("/")[-1]
                    f = file.read()
                    bytes = bytearray(f)
                    file.close()
                    self.upload_file(name, bytes)
                    print("File uploaded")

                elif command == "download":
                    print("Download command received")
                    client_socket.sendall(util.s_to_bytes("OK"))
                    file_id = int(util.s_from_bytes(client_socket.recv(util.bufsize)))

                    self.download_file(file_id)
                    print("File downloaded")

                elif command == "delete":
                    print("Delete command received")
                    client_socket.sendall(util.s_to_bytes("OK"))
                    file_id = int(util.s_from_bytes(client_socket.recv(util.bufsize)))

                    self.delete_file(file_id)
                    print("File deleted")

                else:
                    print("Unrecognized command")

                client_socket.close()

            except Exception as e:
                print(str(e))

        self.command_socket.close()

    def get_total_size(self):
        return sum([x.storage_space for x in self.nodes])

    def get_node_space_available(self, node):
        file_part_sizes = [part.size for part in FilePart.get_file_parts() if part.node_id == node.id] + \
                          [part.size for part in FilePart.get_lost_file_parts() if part.node_id == node.id]
        return node.storage_space - sum(file_part_sizes)

    def get_total_space_available(self):
        return sum([self.get_node_space_available(x) for x in self.nodes])


def main(args):
    # set up the master controller
    master = Master()
    master.listen_for_commands()

    while True:
        time.sleep(60)
    return


instance = None

if __name__ == "__main__":
    main(sys.argv)
