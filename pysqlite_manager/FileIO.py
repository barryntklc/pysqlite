from Objects.NodeList import NodeList
from Objects.PropertyList import PropertyList

class FileIO:

    def READ_Clustercfg(CLUSTERCFG):
        Nodes = NodeList()
        Properties = PropertyList()

        print("[FileIO] Loading node configuration...")
        clustercfgFile = open(CLUSTERCFG, 'r')
        #print(clustercfgFile.read())
        for line in clustercfgFile:
            if line[0] != '#' and '=' in line:
                if '.' in line:
                    prekey, val = line.split('=')
                    val = val.strip('\n\r')
                    nodename, key = prekey.split('.')
                    Nodes.Add(nodename.strip(), key.strip(), val.strip())
                else:
                    key, val = line.split('=')
                    Properties.Add(key.strip(), val.strip())
        clustercfgFile.close()

        return Nodes, Properties

    def READ_SQLFile(SQLFILE):
        Buffer = ''

        print("[FileIO] Reading queries from file...")
        sqlFile = open(SQLFILE, 'r')
        for line in sqlFile:
            if line[0] != '#' and line[:2:] != '--' and line.strip(' \t\n\r') != '':
                Buffer += line
        sqlFile.close()
        print()
        return Buffer

    # https://docs.python.org/3/library/csv.html
    def READ_CSVFile(CSVFILE):
        #lexer = pysqlite_manager.CSVLexer(open(CSVFILE, 'r'))

        #CommonTokenStream

        print("[FileIO] Reading comma separated values from file...")
        import csv
        with open(CSVFILE, newline='') as csvfile:
            filereader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for (row) in filereader:
                print(row)
