import pysqlite_manager
from _winapi import NULL
from .Node import Node
# NodeList
# Stores the connection information for a bunch of Nodes.
#
class NodeList(object):
    Nodes = []

    def __init__(self):
        self.Nodes = []
        # print("Nodelist created.")

    # Add
    # Adds a Node to the NodeList if does not yet exist. Otherwise, modifies the specified attribute of each
    # Node in the NodeList if it does.
    #
    def Add(self, name, key="", val=""):
        if NodeList.Contains(self, name):
            for SelNode in self.Nodes:
                if SelNode.GetName() == name:
                    if key == 'ip':
                        SelNode.SetIPAddr(val)
                    if key == 'port':
                        SelNode.SetPortNum(val)
        else:
            NewNode = Node(name)
            if key == 'ip':
                NewNode.SetIPAddr(val)
            if key == 'port':
                NewNode.SetPortNum(val)
            self.Nodes.append(NewNode)

    # Contains
    # Determines if the list of nodes contains a node with a specific name.
    #
    def Contains(self, name):
        for SelNode in self.Nodes:
            if SelNode.GetName() == name:
                return True
        return False

    # Size
    # Returns the size of the NodeList.
    #
    def Size(self):
        return len(self.Nodes)

    # NumNodes
    # Counts the number of catalog and normal nodes in the cluster config.
    #
    def NumNodes(self):
        cat_counter = 0
        node_counter = 0
        for SelNode in self.Nodes:
            if SelNode.GetName() == 'cat':
                cat_counter += 1
            else:
                node_counter += 1
        return cat_counter, node_counter

    # Get
    # Gets a node by its name.
    #
    def Get(self, name):
        for SelNode in self.Nodes:
            if SelNode.GetName() == name:
                return SelNode
        return -1

    # GetCat
    # Gets the node with cat as its name.
    #
    def GetCat(self) -> Node:
        for SelNode in self.Nodes:
            if SelNode.GetName() == 'cat':
                return SelNode
        return NULL

    def GetNodes(self):
        NodeBuffer = []
        for SelNode in self.Nodes:
            if SelNode.GetName() != 'cat':
                NodeBuffer.append(SelNode)
        return NodeBuffer

    # ToString
    # Returns a string representation of the NodeList.
    #
    def ToString(self):
        buffer = ""
        for self.Node in self.Nodes:
            buffer += self.Node.ToString() + '\n'
        return buffer, self.Size()
