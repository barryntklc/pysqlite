import socket
import sys
import queue
import threading

import pickle

from FileIO import FileIO
from Catalog import Catalog
from Cluster import Cluster

Query = ''

global Nodes
global catalog
global cluster

def Init():
    terminal = 0
    cfgpath = 'dbconfig.txt'  # a path to a database config file
    csvpath = ''
    sqlpath = 'queries.sql'  # a path to query config file
    querycustom = ''  # a custom query

    if len(sys.argv) > 1:
        if sys.argv.__contains__('-t'):
            terminal = 1
        else:
            if sys.argv.__contains__('-c'):
                cfgpath = sys.argv[sys.argv.index('-c') + 1]
            if sys.argv.__contains__('-csv'):
                csvpath = sys.argv[sys.argv.index('-csv') + 1]
            if sys.argv.__contains__('-sql'):
                sqlpath = sys.argv[sys.argv.index('-sql') + 1]
            if sys.argv.__contains__('-sqlc'):
                querycustom = sys.argv[sys.argv.index('-sqlc') + 1]
    else:
        terminal = 1

    return terminal, cfgpath, sqlpath, csvpath, querycustom


def Main():
    print("Starting RunSQL Client")
    terminal, clustercfg, querycfg, csvconfig, querycustom = Init()
    if terminal == 1:
        print("Running in terminal mode.")
        Console()
    else:
        print("Loading items from sql file.")
        Preload(clustercfg, querycfg)


def Preload(cfgpath, sqlpath):
    global Nodes
    global catalog

    valid = True

    ###
    # Read the ClusterCFG
    try:
        Nodes, Properties = FileIO.READ_Clustercfg(cfgpath)
        PrintConnections(Nodes)
        print(Properties.ToString())
        NumCats, NumNodes = Nodes.NumNodes()
        # if NumCats < 1:
        #     print('ERROR: Please define a catalog database!')
        #     valid = False
        # else:
        #     InitCatalog(Nodes)
        #     if NumNodes < 1:
        #         print('ERROR: Please define database nodes!')
        #         valid = False
        #

    except Exception as e:
        # print the error message in string form
        print(str(e))
        valid = False

    try:
        catalog = Catalog(Nodes.GetCat())
        cluster = Cluster(Nodes.GetNodes())
    except Exception as e:
        print(str(e))
        valid = False

    ###
    # Read the SQLFile
    try:
        global Query
        Query = FileIO.READ_SQLFile(sqlpath)
        PrintQueries(Query)
    except OSError:
        print("ERROR: Could not find a query configuration file!")
        valid = False

    ###
    # Run Queries
    if valid is True:
        RunQuery(Nodes, Query)
    else:
        print("An error was encountered. Query file will not be run: quitting.")


def Console():
    while True:
        args = input("> ")
        if args == 'exit' or args == 'close' or args == 'break':
            break
        else:
            print("Still in development. Your input: ", args)


def PrintConnections(nodes):
    values, numnodes = nodes.ToString()

    print('Read ' + str(numnodes) + ' nodes in the configuration file.')
    print(values);


def PrintQueries(queries):
    print('Found the following queries in the specified query file:')
    print(queries);


def InitCatalog(nodes):
    print("Checking catalog database if it does not exist...")
    # TODO set up catalog database

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

    RUNQUERY(nodes.GetCat(), create_table, 1000)


# based on example from
# https://shakeelosmani.wordpress.com/2015/04/13/python-3-socket-programming-example/
#
def RUNQUERY(SelNode, code, query):
    #global Query
    #code = str(1000)

    nodesocket = socket.socket()  # defines the socket object
    nodesocket.connect((SelNode.ipaddr, int(SelNode.portnum)))  # attempts to connect the socket object

    #nodesocket.sendto(query.encode('utf-8'), (SelNode.ipaddr, int(SelNode.portnum)))  # socket object attempts to send the Queries string
    #nodesocket.send(query.encode('utf-8'))  # socket object attempts to send the Queries string
    #nodesocket.send(code.encode('utf-8'))
    nodesocket.send(pickle.dumps((code, query)))
    data = nodesocket.recv(1024)
    array = pickle.loads(data)

    data = array[1]  # data picks up the response
    #data = nodesocket.recv(4096).decode()  # data picks up the response
    status = array[0]
    #status = nodesocket.recv(4096).decode()
    print(status)
    # print ('status = ' + status)

    nodesocket.close()  # socket closes
    return data


def UpdateCatalog(nodeid, nodename, nodeurl, tname, partmtd, partcol, partparam1, partparam2):
    global Nodes

    print('Updating catalog')

    #update_table = 'INSERT INTO dtables VALUES ({0}, \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\', \'{7}\') ON DUPLICATE KEY UPDATE nodeurl=\'{2}\', tname=\'{3}\', partmtd=\'{4}\', partcol=\'{5}\', partparam1=\'{6}\', partparam2=\'{7}\');'.format(
    #    nodeid, nodename, nodeurl, tname, partmtd, partcol, partparam1, partparam2)
    update_table = 'INSERT OR IGNORE INTO dtables VALUES ({0}, \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\', \'{7}\');'.format(
        nodeid, nodename, nodeurl, tname, partmtd, partcol, partparam1, partparam2)

    RUNQUERY(Nodes.GetCat(), update_table, 1003)

    print(update_table)

# from threading example:
# https://stackoverflow.com/questions/2846653/how-to-use-threading-in-python
#
def RunQuery(Nodes, Query):

    print("[RunQueries] Running queries on all nodes.")

    threadqueue = queue.Queue()

    for SelNode in Nodes.Nodes:
        if SelNode.GetName() != 'cat':
            thread = threading.Thread(target=ProcessNode(SelNode, Query, threadqueue))
            thread.daemon = True
            thread.start()

    results = threadqueue.get()
    print('Query Results:')
    print(results)


def ProcessNode(SelNode, Query, threadqueue):
    print('Connecting to ' + SelNode.GetName() + ' at ' + SelNode.ipaddr + ':' + SelNode.portnum)
    data = RUNQUERY(SelNode, Query, 1002)
    threadqueue.put(data)

    UpdateCatalog(1, SelNode.name, SelNode.ipaddr + ':' + SelNode.portnum, '', '', 'partcol', 'partparam1',
                   'partparam2')

Main()
