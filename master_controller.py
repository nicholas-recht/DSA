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


class ConnectionInfo:
    def __init__(self):
        self.socket = None
        self.address = None


class SlaveNode:
    def __init__(self):
        self.socket = None
        self.address = None
        self.id = -1
        self.storage_space = 0
        self.status = "not_set"

        # locks
        self.socket_lock = threading.Lock()

    def to_string(self):
        return str(self.id) + " " + str(self.storage_space) + " " + str(self.address)

    @staticmethod
    def get_slave_nodes(status=None):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        if status is None:
            c.execute("SELECT * FROM tbl_slave_node")
        else:
            params = (status,)
            c.execute("SELECT * FROM tbl_slave_node WHERE status = ?", params)

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
        self.status = None

    def to_string(self):
        return str(self.id) + " " + self.name + " " + util.datetime_to_s(self.upload_date) + ' ' + str(self.status)

    @staticmethod
    def get_files():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_file WHERE status IS NULL OR status != 'lost'")

        rows = c.fetchall()
        files = []
        for row in rows:
            file = File()
            file.id = row[0]
            file.name = row[1]
            file.size = row[2]
            file.upload_date = util.datetime_from_s(row[3])
            file.folder_id = row[4]
            file.status = row[5]
            files.append(file)

        conn.close()

        return files

    @staticmethod
    def get_lost_files():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_file WHERE status = 'lost'")

        rows = c.fetchall()
        files = []
        for row in rows:
            file = File()
            file.id = row[0]
            file.name = row[1]
            file.size = row[2]
            file.upload_date = util.datetime_from_s(row[3])
            file.folder_id = row[4]
            file.status = row[5]
            files.append(file)

        conn.close()

        return files

    @staticmethod
    def get_danger_files():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_file WHERE status = 'danger'")

        rows = c.fetchall()
        files = []
        for row in rows:
            file = File()
            file.id = row[0]
            file.name = row[1]
            file.size = row[2]
            file.upload_date = util.datetime_from_s(row[3])
            file.folder_id = row[4]
            file.status = row[5]
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
            file.status = row[5]

        conn.close()

        return file

    @staticmethod
    def insert_file(file):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (file.name, file.size, util.datetime_to_s(file.upload_date), file.folder_id, file.status)

        c.execute('''INSERT INTO tbl_file
                         (  name,
                            size,
                            upload_date,
                            folder_id,
                            status)
                         VALUES
                         (  ?,
                            ?,
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

        params = (file.name, file.size, file.folder_id, file.status, file.id)

        c.execute('''UPDATE tbl_file SET
                        name = ?,
                        size = ?,
                        folder_id = ?,
                        status = ?
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
        elif folder.id == 1:
            raise Exception("Cannot move the root folder")

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
        self.status = None

    @staticmethod
    def get_file_parts():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_file_part WHERE status IS NULL OR status != 'lost'")

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
            part.status = row[6]
            file_parts.append(part)

        conn.close()

        return file_parts

    @staticmethod
    def get_lost_file_parts():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute("SELECT * FROM tbl_file_part WHERE status = 'lost'")

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
            part.status = row[6]
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
            part.status = row[6]

        conn.close()

        return part

    @staticmethod
    def insert_file_part(part):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (part.file_id, part.node_id, part.access_name, part.sequence_order, part.size, part.status)

        c.execute('''INSERT INTO tbl_file_part
                             (  file_id,
                                node_id,
                                access_name,
                                sequence_order,
                                size,
                                status)
                             VALUES
                             (  ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?)''', params)
        part.id = c.lastrowid
        conn.commit()
        conn.close()

    @staticmethod
    def update_file_part(part):
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        params = (part.file_id, part.node_id, part.access_name, part.sequence_order, part.size, part.status, part.id)

        c.execute('''UPDATE tbl_file_part SET
                            file_id = ?,
                            node_id = ?,
                            access_name = ?,
                            sequence_order = ?,
                            size = ?,
                            status = ?
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
    def clear_db():
        # open the connection
        conn = sqlite3.connect(util.database)
        c = conn.cursor()

        c.execute('''DELETE FROM tbl_file_part
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

            client = ConnectionInfo()
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
            connected_nodes = SlaveNode.get_slave_nodes("connected")
            self.nodes = connected_nodes

        # set up the welcoming socket for new threads
        print("Create welcome thread")
        welcome_activity = WelcomeSocket(new_client_callback=self.accept_new_node)
        welcome_thread = threading.Thread(target=welcome_activity.open_welcome_conn, daemon=True)
        welcome_thread.start()

        # start the synchronization period for already connected nodes
        print("Start synchronization period")
        wait_time = util.restart_window
        while len([x for x in self.nodes if x.status == "connected"]) > 0 and wait_time > 0:
            wait_time -= util.wait_interval
            time.sleep(util.wait_interval)
        print("End synchronization period")

        # get our set of nodes to start with
        for node in self.nodes:
            if node.status == "connected":  # node never reconnected, is now lost
                self.lose_node(node)
            else:  # either it's a new node, or an existing one that has reconnected
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
            node.socket_lock.acquire()
            # send connect command
            node.socket.sendall(util.s_to_bytes("OPEN"))
            ready = select.select([node.socket], [], [], util.master_continuous_wait - .2)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"

            node.socket_lock.release()

            if response != "OPEN":
                raise socket.error("invalid response")
        except socket.error:
            # give it time to reconnect/recover
            node.status = "recovery"
            print("Recovery mode")
            time.sleep(util.slave_reconnect_window)
            if node.status == "recovery":
                self.lose_node(node)

    def lose_node(self, node):
        node.status = "lost"

        # remove the node from the array
        self.nodes_lock.acquire()
        self.nodes.remove(node)
        self.nodes_lock.release()

        SlaveNode.update_slave_node(node)

        print("Node lost")
        # PANIC MODE!!!! -- need to back up all of the file parts that only existed on this node
        all_parts = FilePart.get_file_parts()
        other_parts = []
        node_parts = []
        # separate the parts contained in the node and not
        for part in all_parts:
            if part.node_id == node.id:
                node_parts.append(part)
            else:
                other_parts.append(part)

        # set each of these parts to be lost
        for part in node_parts:
            part.status = 'lost'
            FilePart.update_file_part(part)

        # group the other parts by the file_id and sequence_number
        file_part_map = {}
        for part in other_parts:
            if part.file_id not in file_part_map:
                file_part_map[part.file_id] = {}
            if part.sequence_order not in file_part_map[part.file_id]:
                file_part_map[part.file_id][part.sequence_order] = []
            file_part_map[part.file_id][part.sequence_order].append(part)

        # try to recover any parts on the node that can be
        for part in node_parts:
            try:
                # if the file_id isn't in the map, then no other copies exist and the file is lost
                if part.file_id not in file_part_map or part.sequence_order not in file_part_map[part.file_id]:
                    raise Exception("No other copies of a necessary part exist")

            except Exception as e:
                file = File.get_file(part.file_id)
                print("File (" + file.to_string() + ") lost: " + str(e))
                file.status = "lost"
                File.update_file(file)
                continue

            try:
                # if we don't have enough copies to reach the redundancy level, we need try
                # and back up what we can
                cur_level = len(file_part_map[part.file_id][part.sequence_order])
                if cur_level < util.redundant_level:
                    # get the nodes that don't contain a copy
                    copy_ids = [y.node_id for y in file_part_map[part.file_id][part.sequence_order]]
                    nodes = [x for x in self.nodes if x.id not in copy_ids]
                    if len(nodes) + cur_level < util.redundant_level:
                        raise Exception("Not enough nodes exist to reach the redundancy level")
                    else:
                        # try and make new copies
                        copy_part = file_part_map[part.file_id][part.sequence_order][0]
                        node = self.get_connected_node(copy_part.node_id)
                        rtn = [None] * 1
                        self.download_part(node, copy_part.access_name, rtn, 0)
                        if isinstance(rtn[0], str):
                            raise Exception(rtn[0])
                        else:
                            contents = rtn[0]
                            # copy the file_part to each other node
                            while cur_level < util.redundant_level:
                                node = nodes[-1]
                                nodes.pop()
                                self.upload_part(node, copy_part.access_name, contents, rtn, 0)
                                if rtn[0] is not None:
                                    raise Exception(rtn[0])
                                new_part = FilePart()
                                new_part.node_id = node.id
                                new_part.file_id = copy_part.file_id
                                new_part.access_name = copy_part.access_name
                                new_part.sequence_order = copy_part.sequence_order
                                new_part.size = copy_part.size
                                FilePart.insert_file_part(new_part)

                                cur_level += 1

                # else we're good

            except Exception as e:
                file = File.get_file(part.file_id)
                print("File (" + file.to_string() + ") in danger: " + str(e))
                file.status = "danger"
                File.update_file(file)
                continue

        return

    def accept_new_node(self, node):
        # kick off a new thread to handle the initial handshake
        handshake_thread = threading.Thread(target=self.handshake_node, args=(node,))
        handshake_thread.start()

    def connect_node_node(self, connection_info):
        """
        This function should be run in it's own thread
        :param node:
        :return:
        """
        node = SlaveNode()
        node.address = connection_info.address
        node.socket = connection_info.socket
        node.status = "connected"
        SlaveNode.insert_slave_node(node)

        # send id to the node
        node.socket.sendall(util.i_to_bytes(node.id))
        # get the storage space of the new node
        node.storage_space = util.i_from_bytes(node.socket.recv(util.bufsize))

        SlaveNode.update_slave_node(node)

        if not self.ready:
            node.status = "new"

        # add the node to the list of nodes
        # LOCK
        self.nodes_lock.acquire()
        # CRITICAL SECTION
        self.nodes.append(node)
        # UNLOCK
        self.nodes_lock.release()

    def handshake_node(self, connection_info):
        """
        This function should be run in it's own thread.
        :param node:
        :return:
        """
        id = util.i_from_bytes(connection_info.socket.recv(util.bufsize))

        # if id == -1 then it's a new node
        if id == -1:
            self.connect_node_node(connection_info)
            print("New node connected")
        else:
            # check the case where connecting node doesn't give a valid id
            existing_node = SlaveNode.get_slave_node(id)
            if existing_node is None:
                # treat it like a new node
                self.connect_node_node(connection_info)
                print("Invalid node connected")
            else:
                if existing_node.status == "lost":  # the prodigal son has returned!
                    existing_node.status = "connected"
                    existing_node.address = connection_info.address
                    existing_node.socket = connection_info.socket
                    # send id to the node
                    existing_node.socket.sendall(util.i_to_bytes(existing_node.id))
                    # get the storage space of the new node
                    existing_node.storage_space = util.i_from_bytes(existing_node.socket.recv(util.bufsize))

                    # update all parts on the node to no longer be lost
                    parts = [part for part in FilePart.get_lost_file_parts() if part.node_id == existing_node.id]
                    for part in parts:
                        part.status = None
                        FilePart.update_file_part(part)

                    # search each lost file (and file in danger) and see if those files are no longer lost/in danger
                    lost_files = File.get_lost_files() + File.get_danger_files()
                    for file in lost_files:
                        # get the max sequence_number
                        all_parts = [part.sequence_order for part in FilePart.get_file_parts() if part.file_id == file.id] + \
                                    [part.sequence_order for part in FilePart.get_lost_file_parts() if part.file_id == file.id]
                        max_sequence_number = max(all_parts)

                        # map the file parts by sequence_number to make sure we have enough copies of each
                        parts = [part for part in FilePart.get_file_parts() if part.file_id == file.id]
                        order_map = {}
                        for part in parts:
                            if part.sequence_order not in order_map:
                                order_map[part.sequence_order] = 0
                            order_map[part.sequence_order] += 1

                        # make sure there aren't any missing sequence numbers
                        all_accounted_for = True
                        redundancy_level = True
                        for sequence_number in range(max_sequence_number + 1):
                            if sequence_number not in order_map:
                                all_accounted_for = False
                                redundancy_level = False
                                break
                            elif order_map[sequence_number] < util.redundant_level:
                                redundancy_level = False
                        # do we have enough copies to have the redundancy level?
                        if redundancy_level:
                            file.status = None
                            File.update_file(file)
                        # do we have at least enough to no longer be a lost file?
                        elif all_accounted_for:
                            file.status = "danger"
                            File.update_file(file)

                    SlaveNode.update_slave_node(existing_node)
                    # add the node to the list of nodes
                    # LOCK
                    self.nodes_lock.acquire()
                    # CRITICAL SECTION
                    self.nodes.append(existing_node)
                    # UNLOCK
                    self.nodes_lock.release()
                    print("Lost node connected")
                else:
                    node = self.get_connected_node(id)

                    if node.status == "recovery":  # good to go
                        node.status = "connected"
                        node.address = connection_info.address
                        node.socket = connection_info.socket
                        # send id to the node
                        node.socket.sendall(util.i_to_bytes(node.id))
                        # get the storage space of the new node
                        node.storage_space = util.i_from_bytes(node.socket.recv(util.bufsize))

                        SlaveNode.update_slave_node(node)
                        print("Recovered node connected")
                    else:
                        if self.ready:  # during normal execution
                            # why are we getting a new connection?? We assume it's an impostor
                            self.connect_node_node(connection_info)
                            print("Impostor node connected")
                        else:  # during the connection window
                            node.status = "connected"
                            node.address = connection_info.address
                            node.socket = connection_info.socket
                            # send id to the node
                            node.socket.sendall(util.i_to_bytes(node.id))
                            # get the storage space of the new node
                            node.storage_space = util.i_from_bytes(node.socket.recv(util.bufsize))

                            SlaveNode.update_slave_node(node)
                            node.status = "restart"
                            print("Existing node connected")

    def get_connected_node(self, id):
        matches = [x for x in self.nodes if x.id == id]
        if len(matches) != 1:
            raise Exception("Error: existing node not contained in connected node list")
        else:
            return matches[0]

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
                       status TEXT,
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
                       status TEXT,
                       FOREIGN KEY(file_id) REFERENCES tbl_file(id) ON UPDATE CASCADE,
                       FOREIGN KEY(node_id) REFERENCES tbl_slave_node(id))'''))

        # c.execute('''CREATE UNIQUE INDEX ux_tbl_file_part ON tbl_file_part(file_id, sequence_order)''')

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
            node.socket_lock.acquire()
            node.socket.sendall(util.s_to_bytes("CLOSE"))
            node.socket_lock.release()
        self.execute = False

    def upload_file(self, name, bytes, folder_id=1):
        # find which nodes have enough space to be used to store the file
        file_size = len(bytes)
        redundant_level = util.redundant_level
        nodes = []
        if len(self.nodes) == 1:
            redundant_level = 1

        while redundant_level > 0:
            enough_space = False
            nodes = [node for node in self.nodes]
            while len(nodes) > 0:
                split_size = math.ceil(file_size / len(nodes)) * redundant_level
                all_good = True
                for node in nodes:
                    if self.get_node_space_available(node) < split_size:
                        all_good = False
                        nodes.remove(node)
                        break
                if all_good:
                    enough_space = True
                    break
            if enough_space:
                break
            else:
                redundant_level -= 1

        if redundant_level == 0:
            raise Exception("Not enough space available")
        elif len(nodes) == 1:
            redundant_level = 1

        # create the new file object
        file_obj = File()
        file_obj.folder_id = folder_id
        file_obj.upload_date = datetime.datetime.utcnow()
        file_obj.name = name
        file_obj.size = len(bytes)
        if redundant_level == 1:
            file_obj.status = "danger"

        File.insert_file(file_obj)

        # split the file into parts
        num_splits = len(nodes)
        if num_splits > 0:
            split_size = math.ceil(file_obj.size / num_splits)
            # make a copy for each redundant_level
            for i in range(redundant_level):
                index = 0
                array_index = 0
                parts = [None] * num_splits
                part_bytes = [None] * num_splits
                while index < num_splits:
                    part_bytes[index] = bytes[array_index:array_index + split_size]

                    part = FilePart()
                    part.node_id = nodes[index].id
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
                                         args=(nodes[index],
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
                    errmsg = ""
                    for error in errors:
                        errmsg += error
                    File.delete_file(file_obj)
                    raise Exception("Error sending file: " + errmsg)
                else:
                    for part in parts:
                        FilePart.insert_file_part(part)

                # rotate all of the nodes so we don't put the same file parts in each
                rotate_nodes = [None] * num_splits
                for i_node in range(num_splits):
                    rotate_nodes[i_node] = nodes[(i_node + 1) % num_splits]
                nodes = rotate_nodes

            # return the file_obj if everything was successfully completed
            return file_obj

        else:
            File.delete_file(file_obj)
            raise Exception("Error: no connected nodes")

        return None

    def upload_part(self, node, name, bytes, errors, index):
        node.socket_lock.acquire()
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

        finally:
            node.socket_lock.release()

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
                msg = ""
                errors = [x for x in rtnVal if isinstance(x, str)]
                for error in errors:
                    msg += error
                raise Exception("Error receiving file: " + msg)
            else:
                file_contents = bytearray()
                for part_content in part_contents:
                    file_contents += part_content

                return file_contents

        except Exception as e:
            print(str(e))

            return None

    def download_part(self, node, name, rtn_val, index):
        node.socket_lock.acquire()
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

            rtn_val[index] = bytes

        except socket.error as e:
            rtn_val[index] = str(e)

        finally:
            node.socket_lock.release()

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
            rtn_val = []
            threads = []
            parts = []
            index = 0
            for part in file_parts:
                if part.node_id in node_dict:
                    rtn_val.append(None)
                    parts.append(part)
                    t = threading.Thread(target=self.delete_part,
                                         args=(node_dict[part.node_id],
                                               part.access_name,
                                               rtn_val,
                                               index))
                    threads.append(t)
                    t.start()
                else:
                    FilePart.delete_file_part(part)
                index += 1

            for t in threads:
                t.join()
            self.busy = False

            # check for any errors
            for idx, val in enumerate(rtn_val):
                part = parts[idx]
                FilePart.delete_file_part(part)

                if isinstance(val, str):
                    print(val)

            File.delete_file(file_obj)

        except Exception as e:
            print(str(e))

            return None

    def delete_part(self, node, name, errors, index):
        node.socket_lock.acquire()
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

        finally:
            node.socket_lock.release()

    def search_files(self, substr):
        file_ids = set()
        # search each active node for their file parts
        self.busy = True
        rtn_val = []
        threads = []
        index = 0
        for node in self.nodes:
            rtn_val.append(None)
            t = threading.Thread(target=self.search_file_parts,
                                 args=(node,
                                       substr,
                                       rtn_val,
                                       index))
            threads.append(t)
            t.start()
            index += 1

        for t in threads:
            t.join()
        self.busy = False

        # check for any errors
        for idx, val in enumerate(rtn_val):
            if isinstance(val, str):
                print(val)
            else:
                # get all of the unique files
                for part in val:
                    if part.file_id not in file_ids:
                        file_ids.add(part.file_id)

        files = [file for file in File.get_files() if file.id in file_ids]

        return files

    def search_file_parts(self, node, substr, rtn_val, index):
        node.socket_lock.acquire()
        try:
            # send command
            node.socket.sendall(util.s_to_bytes("SEARCH"))
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"
            if response != "OK":
                raise socket.error("upload time out")
            # send the name
            node.socket.sendall(substr)
            ready = select.select([node.socket], [], [], util.slave_response_timeout)
            if ready[0]:
                response = util.s_from_bytes(node.socket.recv(util.bufsize))
            else:
                response = "FAIL"

            if response == "NONE":
                rtn_val[index] = []
            else:
                names = response.split(",")
                files = [part for part in FilePart.get_file_parts() if part.access_name in names]

                rtn_val[index] = files

        except socket.error as e:
            rtn_val[index] = str(e)

        finally:
            node.socket_lock.release()

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

                if command == "clear_database":
                    SlaveNode.clear_db()
                    File.clear_db()
                    FilePart.clear_db()
                    Folder.clear_db()
                    print("database cleared")
                elif command == "show_nodes":
                    for node in self.nodes:
                        print(node.to_string())
                elif command == "space_available":
                    print(str(self.get_total_space_available()))
                elif command == "total_space":
                    print(str(self.get_total_size()))
                elif command == "show_files":
                    files = File.get_files()
                    for file in files:
                        print(file.to_string())
                elif command == "show_lost_files":
                    files = File.get_lost_files()
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

                    bytes = self.download_file(file_id)
                    file_obj = File.get_file(file_id)

                    file = open(file_obj.name, mode='wb')
                    file.write(bytes)
                    file.close()

                    print("File downloaded")

                elif command == "delete":
                    print("Delete command received")
                    client_socket.sendall(util.s_to_bytes("OK"))
                    file_id = int(util.s_from_bytes(client_socket.recv(util.bufsize)))

                    self.delete_file(file_id)
                    print("File deleted")

                elif command == "search":
                    print("Search command received")
                    client_socket.sendall(util.s_to_bytes("OK"))
                    search_string = client_socket.recv(util.bufsize)

                    files = self.search_files(search_string)
                    for file in files:
                        print(file.name)

                    print("Search complete")

                else:
                    print("Unrecognized command")

                client_socket.close()

            except Exception as e:
                print(str(e))

        self.command_socket.close()

    def get_total_size(self):
        return sum([x.storage_space for x in self.nodes])

    def get_node_space_available(self, node):
        file_part_sizes = [part.size for part in FilePart.get_file_parts() if part.node_id == node.id]
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
