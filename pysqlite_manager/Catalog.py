import socket

import pickle


class Catalog(object):

    global cat_node;

    create_table = """CREATE TABLE IF NOT EXISTS dtables (
        partid int not null primary key,
        nodename char(32),
        nodeurl char(128),
        tname char(32),
        partmtd int,
        partcol char(32),
        partparam1 char(32),
        partparam2 char(32)
        );"""

    def __init__(self, Node):
        self.cat_node = Node
        if self.cat_node.TestConnection():
            print("Connection to catalog node successful.")

            self.RUNQUERY(self.cat_node, 100, self.create_table)
        else:
            print("Could not connect to catalog node. Please check your configuration.")

    def RUNQUERY(self, SelectedNode, Code, Data):
        nodesocket = socket.socket()  # defines the socket object
        nodesocket.connect((SelectedNode.ipaddr, int(SelectedNode.portnum)))  # attempts to connect the socket object

        nodesocket.send(pickle.dumps((Code, Data)))
        array = pickle.loads(nodesocket.recv(1024))

        data = array[1]  # data picks up the response
        # data = nodesocket.recv(4096).decode()  # data picks up the response
        status = array[0]
        # status = nodesocket.recv(4096).decode()
        print(status)
        # print ('status = ' + status)

        nodesocket.close()  # socket closes
        return data

    #AddPart
        #if a partition doesn't exist, add it
        #if it does, update it

    #RemovePart
        #remove partitions for a given table

