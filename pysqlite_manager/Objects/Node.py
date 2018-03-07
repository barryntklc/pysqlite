import socket

import pickle

import pysqlite_manager

class Node(object):

    def __init__(self, name, ipaddr="", portnum=0):
        self.name = name
        self.ipaddr = ipaddr
        self.portnum = portnum

    def TestConnection(self):
        code = '001'

        nodesocket = socket.socket()  # defines the socket object
        nodesocket.connect((self.ipaddr, int(self.portnum)))  # attempts to connect the socket object

        try:
            nodesocket.send(pickle.dumps((code, "test_connection")))
            reply = pickle.loads(nodesocket.recv(1024))
            #print(reply[0])
            #print(reply[1])
            if reply[0] == '200':
                return True
        except Exception as e:
            return False
        return False

    def GetName(self):
        return self.name

    def GetIPAddr(self):
        return self.ipaddr

    def GetPort(self):
        return self.portnum

    def SetName(self, name):
        self.name = name

    def SetIPAddr(self, ipaddr):
        self.ipaddr = ipaddr

    def SetPortNum(self, portnum):
        self.portnum = portnum

    def ToString(self):
        return '[' + self.name + ']' + ' IP: ' + self.ipaddr + ' PORT: ' + str(self.portnum)