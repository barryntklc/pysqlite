import Objects

class Property(object):

    def __init__(self, key, val):
        self.key = key
        self.val = val

    def GetKey(self):
        return self.key

    def GetVal(self):
        return self.val

    def SetKey(self, key):
        self.key = key

    def SetVal(self, val):
        self.val = val

    def ToString(self):
        return '[' + self.key + ']: ' + self.val