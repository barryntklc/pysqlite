import Objects
from .Property import Property

# NodeList
# Stores the connection information for a bunch of Nodes.
#
class PropertyList(object):
    Properties = []

    def __init__(self):
        self.Properties = []

    # Size
    # Returns the size of the NodeList.
    #
    def Size(self):
        return len(self.Properties)

    # Add
    # Adds a Property to the PropertyList if does not yet exist. Otherwise, modifies the val of each
    # Property in the PropertyList if it does.
    #
    def Add(self, key, val):
        if PropertyList.Contains(self, key):
            for SelProperty in self.Properties:
                if SelProperty.GetKey() == key:
                    SelProperty.SetVal(val)
        else:
            self.Properties.append(Property(key, val))

    # Contains
    # Determines if the PropertyList contains a Property with a specific key.
    #
    def Contains(self, key):
        for SelProperty in self.Properties:
            if SelProperty.GetKey() == key:
                return True
        return False

    # Get
    # Gets a Property by its key and returns its value.
    #
    def Get(self, key):
        for SelProperty in self.Properties:
            if SelProperty.GetKey() == key:
                return SelProperty.GetVal()
        return -1

    # ToString
    # Returns a string representation of the PropertyList.
    #
    def ToString(self):
        buffer = ""
        for self.Property in self.Properties:
            buffer += self.Property.ToString() + '\n'
        return buffer, self.Size()
