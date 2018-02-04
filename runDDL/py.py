import socket 
import sys

CLUSTERCFG = 'dbconfig.txt' 
DDLFILE = 'queries.sql' 

Queries = ''

NODES = []

def Main():
    #print(len(sys.argv)) 
    #todo add argument handling
    #for s in sys.argv:
    #    print(s) 
    #1) custom query mode
    #2) query files mode
    #3) enter queries mode
    
    print("Starting RunDDL Client") 
    valid = True 
    try:
        READ_Clustercfg(CLUSTERCFG) 
    except OSError:
        print("Could not find a node configuration file!") 
        valid = False 
    try:
        global Queries 
        Queries = READ_Ddlfile(DDLFILE)
    except OSError:
        print("Could not find a query configuration file!") 
        valid = False 
    if valid is True:
        NODES_OpenConnections()
    else:
        print("Error encountered, quitting.") 

def READ_Clustercfg(file):
    print("Loading node configuration...") 
    dbconfig = open(file, 'r')
    for line in dbconfig:
        if line[0] != '#' and '=' in line:
            prekey, val = line.split('=') 
            val = val.strip('\n\r')
            nodename, key = prekey.split('.')
            if (NODES_Contains(nodename) is True):
                for NODE in NODES:
                    if (NODE.name == nodename):
                        if (key == 'ip'):
                            NODE.ipaddr = val
                        if (key == 'port'):
                            NODE.portnum = val
            else:
                NODE = Node(nodename)
                if (key == 'ip'):
                    NODE.ipaddr = val
                if (key == 'port'):
                    NODE.portnum = val
                NODES.append(NODE)
    dbconfig.close()
    print('Found ' + str(NODES.__len__()) + ' nodes.') 
    NODES_PrintConnections()

def READ_Ddlfile(file):
    print("Loading query configuration...")
    Buffer = ''
    queries = open(file, 'r')
    for line in queries:
        if line[0] != '#' and line.strip(' \t\n\r') != '':
            Buffer += line
    queries.close()
    return Buffer

def NODES_Contains(node):
    for Node in NODES:
        if (Node.name == node):
            return True
    return False

def NODES_PrintConnections():
    for Node in NODES:
        print(Node.ToString())
        
def NODES_OpenConnections():
    for Node in NODES:
        print('Connecting to ' + Node.name + '...')
        CONNECT(Node.ipaddr, Node.portnum)
        
def NODES_SetupCatalog():
    print("Checking catalog database...")
    #TODO set up catalog database
    
    create_table = """CREATE TABLE IF NOT EXISTS dtables (
    tname char(32),
    nodeid int,
    nodeurl char(128),
    partmtd int,
    partcol char(32),
    partparam1 char(32),
    partparam2 char(32)
    );"""
    
    

class Node(object):
    name = "" 
    ipaddr = "" 
    portnum = 0 
    
    def __init__(self, name):
        self.name = name 
        self.ipaddr = "" 
        self.portnum = 0
        
    def ToString(self):
        return ('[' + self.name + ']' + '\n IP: ' + self.ipaddr + ' PORT: ' + str(self.portnum))

#based on example from 
#https://shakeelosmani.wordpress.com/2015/04/13/python-3-socket-programming-example/
def CONNECT(HOST, PORT):
    global Queries
    
    nodesocket = socket.socket() #defines the socket object
    nodesocket.connect((HOST,int(PORT))) #attempts to connect the socket object

    nodesocket.send(Queries.encode()) #socket object attempts to send the Queries string
    data = nodesocket.recv(1024).decode() #data picks up the response
    print (data)
             
    nodesocket.close() #socket closes

Main()
