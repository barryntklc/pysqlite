from Objects.Node import Node

class Cluster(object):

    global Nodes
    global valid

    def __init__(self, Nodes):
        self.Nodes = Nodes
        self.valid = self.TestConnections()
        if not self.valid:
            print("Could not connect to one of the nodes in the cluster.")

    def TestConnections(self):
        global Nodes

        print("Testing node connections...")

        valid = True

        for Node in self.Nodes:
            try:
                node_online = Node.TestConnection()
                if (node_online):
                    print(Node.ToString() + ": Connection successful.")
                else:
                    print(Node.ToString() + ": Connection failed.")
                    valid = False
            except Exception as e:
                print(Node.ToString() + ": Connection failed.")
                valid = False
        return valid

    def RunQuery(self, sql):
        global Nodes

        valid = True

        for Node in self.Nodes:
            try:
                # node_online = Node.TestConnection()
                # if (node_online):
                #     print(Node.ToString() + ": Connection successful.")
                # else:
                #     print(Node.ToString() + ": Connection failed.")
                #     valid = False
                QueryResults = Node.RunQuery(sql)
                print(str(QueryResults))
                valid = True
            except Exception as e:
                # print(Node.ToString() + ": Connection failed.")
                QueryResults = ""
                valid = False
        return valid, QueryResults

    def CREATE_TABLE(self, query):
        # TODO if valid == True update catalog (return true)
        # ELSE don't update catalog (return false)
        # get table name
        # in each node

        allvalid = True
        allservinfo = []

        for Node in self.Nodes:
            valid, servcode, servdata = Node.RunQuery(query)
            allservinfo.append([Node.GetName(), Node.GetIPAddr(), Node.GetPort(), servcode, servdata])
            if (valid) == False:
                allvalid = False
        return allvalid, allservinfo

    def DROP_TABLE(self, query):
        # get table name
        # add table with name

        allvalid = True
        allservinfo = []

        for Node in self.Nodes:
            valid, servcode, servdata = Node.RunQuery(query)
            allservinfo.append([Node.GetName(), Node.GetIPAddr(), Node.GetPort(), servcode, servdata])
            if (valid) == False:
                allvalid = False
        return allvalid, allservinfo

    def SELECT(self):
        #TODO define select
        print()

    def INSERT(self):
        # get table name
        print()

    def UPDATE(self):
        print()

