class KVPair(object):
    key = ""
    val = ""
    
    def __init__(self):
        self.key = ""
        self.val = ""
    
    def ToString(self):
        return self.key + '=' + self.val
