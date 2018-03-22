import socket
import sys
import queue
import threading

import pickle

import antlr4

from FileIO import FileIO
from Catalog import Catalog
from Cluster import Cluster
from SQLParser import SQLParser

from antlr4 import *
from AntlrSQLite import *
from AntlrSQLite.SQLiteLexer import SQLiteLexer
from AntlrSQLite.SQLiteListener import SQLiteListener
from AntlrSQLite.SQLiteParser import SQLiteParser

QueryFileContents = ''

global Nodes
global Properties
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
    print("Starting PySQLite Manager")
    terminal, clustercfg, querycfg, csvconfig, querycustom = Init()
    if terminal == 1:
        print("Running in terminal mode.")
        Console()
    else:
        print("Loading items from sql file.")
        Preload(clustercfg, querycfg)


def Preload(cfgpath, sqlpath):
    global Nodes
    global Properties
    global catalog
    global cluster

    #valid = True

    ###
    # Read the ClusterCFG
    try:
        Nodes, Properties = FileIO.READ_Clustercfg(cfgpath)
        PrintConnections(Nodes)
        PrintProperties(Properties)
        #NumCats, NumNodes = Nodes.NumNodes() #todo move to connection attempt

    except Exception as e:
        # print the error message in string form
        print(str(e))
        valid = False

    ###
    # Read the SQLFile
    # TODO parse with ANTLR
    try:
        global QueryFileContents
        QueryFileContents = FileIO.READ_SQLFile(sqlpath)
        PrintQueries(QueryFileContents)
    except OSError:
        print("ERROR: Could not find a query configuration file!")
        valid = False

    try:
        catalog = Catalog(Nodes.GetCat())
        cluster = Cluster(Nodes.GetNodes())
    except Exception as e:
        print(str(e))
        #valid = False

    #print(catalog.valid)
    #print(cluster.valid)

    if catalog.valid and cluster.valid:
        print('Parsing SQL Statements...')
        ###
        # Run Queries

        Parse(QueryFileContents)

        #RunQuery(Nodes, Query)
    else:
        print("An error was encountered. Please check your database configuration file,\nand that the specified "
              "pysqlite server nodes are online.")

def Parse(query):
    global catalog
    global cluster

    parser = SQLParser(query)
    ParsedStatements = parser.Parse()
    if ParsedStatements.__len__() is not 0:
        for item in ParsedStatements:
            OperationType = ""
            #OperationType = subitem[0][0]

            print()

            print('Parse results for \"' + str(item[1][0][3]) + '\"')
            ParseTree = item[1]
            for subitem in ParseTree:
                sys.stdout.write(subitem[0] + '\n\t' + str(subitem[2]) + '\t' + ('\t' * subitem[2]) + subitem[3] + '\n')

            query = item[0] + ";"

            OperationType = str(item[1][0][0])
            if OperationType == "CREATE TABLE":
                opstatus = cluster.CREATE_TABLE(query)
                print(opstatus[0])
                if opstatus[0]:
                    print("Partition successful.")
                    print("Updating catalog.")
                    catalog.AddPart(item, opstatus)
                else:
                    print("Could not create new partitions.")
                    for logitem in opstatus[1]:
                        print(logitem)

            elif OperationType == "DROP TABLE":
                print('FOUND ' + OperationType)
                opstatus = cluster.DROP_TABLE(query)
                print(opstatus[0])
                if opstatus[0]:
                    print("Partition dropped.")
                    print("Updating catalog.")

                    catalog.RemovePart(item, opstatus)
                else:
                    print("Could not drop partitions.")
                    for logitem in opstatus[1]:
                        print(logitem)
    else:
        print("Did not find any queries! Quitting.")

    #runquery on cluster

    #lexer = SQLiteLexer(query)

    #lexer = antlr4.Lexer(antlr4.InputStream(query))
    #stream = antlr4.CommonTokenStream(lexer)
    #parser = SQLiteParser(stream)
    #print lexer.
    #print(parser.drop_table_stmt())
    #print(parser.alter_table_stmt())
    #print(parser.create_table_stmt())

def Console():
    while True:
        args = input("> ")
        if args == 'exit' or args == 'close' or args == 'break':
            break
        else:
            print("Still in development. Your input: ", args)

def PrintConnections(nodes):
    values, numnodes = nodes.ToString()
    print('Found ' + str(numnodes) + ' connection(s) in the configuration file.')
    print(values)


def PrintProperties(properties):
    values, numproperties = properties.ToString()
    print('Found ' + str(numproperties) + ' propertie(s) in the configuration file.')
    print(values)


def PrintQueries(queries):
    print('Found the following queries in the specified query file:')
    print(queries)


# based on example from
# https://shakeelosmani.wordpress.com/2015/04/13/python-3-socket-programming-example/
# TODO do this in catalog/cluster instead
# def RUNQUERY(SelNode, code, query):
#
#     nodesocket = socket.socket()  # defines the socket object
#     nodesocket.connect((SelNode.ipaddr, int(SelNode.portnum)))  # attempts to connect the socket object
#
#     nodesocket.send(pickle.dumps((code, query)))
#     data = nodesocket.recv(1024)
#     array = pickle.loads(data)
#
#     data = array[1]  # data picks up the response
#     #data = nodesocket.recv(4096).decode()  # data picks up the response
#     status = array[0]
#     #status = nodesocket.recv(4096).decode()
#     print(status)
#     # print ('status = ' + status)
#
#     nodesocket.close()  # socket closes
#     return data


# def UpdateCatalog(nodeid, nodename, nodeurl, tname, partmtd, partcol, partparam1, partparam2):
#     global Nodes
#
#     print('Updating catalog')
#
#     update_table = 'INSERT OR IGNORE INTO dtables VALUES ({0}, \'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\', \'{7}\');'.format(
#         nodeid, nodename, nodeurl, tname, partmtd, partcol, partparam1, partparam2)
#
#     RUNQUERY(Nodes.GetCat(), 103, update_table)
#
#     print(update_table)

# from threading example:
# https://stackoverflow.com/questions/2846653/how-to-use-threading-in-python
#
# def RunQuery(Nodes, Query):
#
#     print("[RunQueries] Running queries on all nodes.")
#
#     threadqueue = queue.Queue()
#
#     for SelNode in Nodes.Nodes:
#         if SelNode.GetName() != 'cat':
#             thread = threading.Thread(target=ProcessNode(SelNode, Query, threadqueue))
#             thread.daemon = True
#             thread.start()
#
#     results = threadqueue.get()
#     print('Query Results:')
#     print(results)


# def ProcessNode(SelNode, Query, threadqueue):
#     print('Connecting to ' + SelNode.GetName() + ' at ' + SelNode.ipaddr + ':' + SelNode.portnum)
#     data = RUNQUERY(SelNode, 102, Query)
#     threadqueue.put(data)
#
#     UpdateCatalog(1, SelNode.name, SelNode.ipaddr + ':' + SelNode.portnum, '', '', 'partcol', 'partparam1',
#                    'partparam2')

Main()
