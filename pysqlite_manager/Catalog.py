import socket
import pickle

class Catalog(object):

    global CatNode
    global valid

    def __init__(self, Node):
        self.CatNode = Node
        self.valid = True

        print("Testing catalog node connection...")

        self.valid = self.CatNode.TestConnection()
        if self.valid:
            print(self.CatNode.ToString() + ": Connection successful.")
            print("Connected to the catalog node.")
            if self.InitCatalog():
                print("Catalog node table initialized.")
            else:
                print("Catalog node table failed to initialize.")
        else:
            print(self.CatNode.ToString() + ": Connection failed.")
            print("Could not connect to catalog node.")

    def InitCatalog(self):
        create_catalog = """CREATE TABLE IF NOT EXISTS dtables (
partid integer not null primary key autoincrement,
nodename char(32),
nodeurl char(128),
tname char(32),
partmtd int,
partcol char(32),
partparam1 char(32),
partparam2 char(32)
);"""
        valid, servcode, servdata = self.CatNode.RunQuery(create_catalog)
        #print(servcode)
        #print(servdata)

        return valid

    def AddPart(self, item, opstatus):
        print("adding partition")
        print(item)
        print(opstatus)
        tname = item[1][0][1]
        for connection in opstatus[1]:
            nodename = connection[0]
            nodeurl = connection[1] + ':' + connection[2]
            partmtd = 0
            partcol = ''
            partparam1 = ''
            partparam2 = ''

            # TODO insert or replace into doesn't work. Will still duplicate a partition in the catalog node if it's able to create the table in the nodes.
            # TODO it probably has to do with the index being autoincremented here
            # TODO Partition management may have to be done here, with constant preloading the table from Catalog and pushing to the catalog
            # It makes sense, considering this isn't actually a "client" but more like the inbetween of server and client. pysqlite manager would also be a server too, theoretically

            add_partition = "INSERT OR REPLACE INTO dtables (nodename, nodeurl, tname, partmtd, partcol, partparam1, " \
                            "partparam2) VALUES ('" + nodename + "', '" + nodeurl + "', '" + tname + "', '" \
                            + str(partmtd) + "', '" + partcol + "', '" + partparam1 + "', '" + partparam2 + "');"
            print(add_partition)

            thisvalid, servcode, servdata = self.CatNode.RunQuery(add_partition)
            print(thisvalid, servcode, servdata)
            # TODO return valid

            # print(connection)
        #nodename =
        #if a partition doesn't exist, add it
        #if it does, update it

    def RemovePart(self, item, opstatus):
        print("removing partition")
        print(item)
        print(opstatus)

        tname = item[1][0][1]
        for connection in opstatus[1]:
            nodename = connection[0]
            nodeurl = connection[1] + ':' + connection[2]
            partmtd = 0
            partcol = ''
            partparam1 = ''
            partparam2 = ''

            delete_partition = "DELETE FROM dtables WHERE nodename = '" + nodename + "' AND nodeurl = '" + nodeurl + \
                               "' AND tname = '" + tname + "';"
            print(delete_partition)

            thisvalid, servcode, servdata = self.CatNode.RunQuery(delete_partition)
            print(thisvalid, servcode, servdata)

            # TODO return valid
