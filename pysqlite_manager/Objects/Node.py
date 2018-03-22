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

        try:
            nodesocket.connect((self.ipaddr, int(self.portnum)))  # attempts to connect the socket object
            nodesocket.send(pickle.dumps((code, "test_connection")))
            reply = pickle.loads(nodesocket.recv(1024))
            if reply[0] == '200':
                return True
            else:
                return False
        except Exception as e:
            return False
        except WindowsError as e:
            return False

    def RunQuery(self, sql):
        code = '100'

        nodesocket = socket.socket()  # defines the socket object
        nodesocket.connect((self.ipaddr, int(self.portnum)))  # attempts to connect the socket object

        result = False
        servcode = ""
        servdata = ""

        try:
            nodesocket.send(pickle.dumps((code, sql)))
            reply = pickle.loads(nodesocket.recv(1024))
            # print("RunQuery servcode: " + reply[0])
            # print("RunQuery servdata: " + reply[1])
            # TODO either handle error here or return both servcode and servdata
            if reply[0] == '202':
                result = True
            else:
                result = False
            servcode = reply[0]
            servdata = reply[1]
        except Exception as e:
            result = False

        return result, servcode, servdata

    #def AddPart:

    #def RemovePart:

    #def ReAlloc:

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