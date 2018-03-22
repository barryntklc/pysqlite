import sys

class SQLParser(object):
    def __init__(self, stdin):
        self.input = stdin

    def Parse(self):
        Buffer = []

        Statements = self.ExtractStatements(self.input)
        for CleanedStatement in Statements:
            if CleanedStatement.count('(') == CleanedStatement.count(')'):
                ParseTree = self.GenerateParseTree(CleanedStatement)
                StatementItem = [CleanedStatement, ParseTree]
                Buffer.append(StatementItem)

        return Buffer

    def GenerateParseTree(self, stdin):
        ParseTree = []

        buffer = ['', '', 0, '']
        index = 0
        depth = 0

        for char in stdin:
            if char == ',':
                if buffer[3] is not '':
                    buffer[3] = buffer[3].strip()
                    buffer[0], buffer[1] = self.GetCommand(buffer[3])
                    ParseTree.append(buffer)
                    buffer = ['', '', depth, '']
            elif char == '(':
                depth += 1
                buffer[3] = buffer[3].strip()
                buffer[0], buffer[1] = self.GetCommand(buffer[3])
                ParseTree.append(buffer)
                buffer = ['', '', depth, '']
            elif char == ')':
                depth -= 1
                buffer[3] = buffer[3].strip()
                buffer[0], buffer[1] = self.GetCommand(buffer[3])
                ParseTree.append(buffer)
                buffer = ['', '', depth, '']
            else:
                buffer[3] = buffer[3] + char
            index += 1
        if buffer[3].strip() is not '':  # catches the last part of the buffer outside parenthesis
            buffer[3] = buffer[3].strip()
            buffer[0], buffer[1] = self.GetCommand(buffer[3])
            ParseTree.append(buffer)

        return ParseTree

    # https://stackoverflow.com/questions/2077897/substitute-multiple-whitespace-with-single-whitespace-in-python
    def ExtractStatements(self, stdin):

        strarr = stdin.split(';')
        buffer = []
        for astr in strarr:
            if astr.strip() is not '':
                astr = astr.replace('\r\n', ' ')
                astr = astr.replace('\n', ' ')
                astr = astr.replace('\t', '')
                astr = astr.replace(';', '')
                astr = ' '.join(astr.split())
                astr = astr.strip()

                buffer.append(astr)
        return buffer

    def GetTableName(self, stdin, type):

        buffer = ""

        type_one = ("CREATE TABLE", "DROP TABLE")  # table name is right after statement
        type_two = ("SELECT", )  # table name is after FROM

        if any((type == phrase) for phrase in type_one):
            layout = stdin.split(' ')
            layout_caps = stdin.upper().split(' ')
            buffer = layout[layout_caps.index(type.split(' ')[len(type.split(' ')) - 1]) + 1]
            print(buffer)
            # TODO support select/from
            # TODO this may require recursion
        elif any((type == phrase) for phrase in type_two):
            # layout = stdin.split(' ')
            # layout_caps = stdin.upper().split(' ')
            # buffer = layout[layout_caps.index('FROM') + 1]
            # print(buffer)
            buffer = ""
        else:
            buffer = ""
        return buffer

    def GetCommand(self, stdin):
        command = ""
        tablename = ""

        # https://www.w3schools.com/sql/sql_syntax.asp
        # https://stackoverflow.com/questions/10149747/and-or-in-python

        select_phrase = ("SELECT", )
        update_phrase = ("UPDATE", )
        delete_phrase = ("DELETE", )
        insert_into_phrase = ("INSERT INTO", )
        create_phrase = ("CREATE DATABASE", "CREATE TABLE", "CREATE INDEX")
        drop_phrase = ("DROP DATABASE", "DROP TABLE", "DROP INDEX")
        alter_phrase = ("ALTER DATABASE", "ALTER TABLE")
        var_phrase = ("INT", "CHAR", "BOOLEAN")

        if any(self.Contains(phrase, stdin) for phrase in select_phrase):
            command = "SELECT"
        elif any(self.Contains(phrase, stdin) for phrase in update_phrase):
            command = "UPDATE"
        elif any(self.Contains(phrase, stdin) for phrase in delete_phrase):
            command = "DELETE"
        elif any(self.Contains(phrase, stdin) for phrase in insert_into_phrase):
            command = "INSERT INTO"
        elif any(self.Contains(phrase, stdin) for phrase in create_phrase):
            if self.Contains("CREATE DATABASE", stdin):
                command = "CREATE DATABASE"
            elif self.Contains("CREATE TABLE", stdin):
                command = "CREATE TABLE"
            elif self.Contains("CREATE INDEX", stdin):
                command = "CREATE INDEX"
        elif any(self.Contains(phrase, stdin) for phrase in drop_phrase):
            if self.Contains("DROP DATABASE", stdin):
                command = "DROP DATABASE"
            elif self.Contains("DROP TABLE", stdin):
                command = "DROP TABLE"
            elif self.Contains("DROP INDEX", stdin):
                command = "DROP INDEX"
        elif any(self.Contains(phrase, stdin) for phrase in alter_phrase):
            if self.Contains("ALTER DATABASE", stdin):
                command = "ALTER DATABASE"
            elif self.Contains("ALTER TABLE", stdin):
                command = "ALTER TABLE"
        elif any(self.Contains(phrase, stdin) for phrase in var_phrase):
            command = "DEF "
            if self.Contains(" INT", stdin):
                command += "INT"
            elif self.Contains(" CHAR", stdin):
                command += "CHAR"
            elif self.Contains(" BOOLEAN", stdin):
                command += "BOOLEAN"
            else:
                command += "OTHER"
        else:
            command = "OTHER"

        tablename = self.GetTableName(stdin, command)

        return command, tablename

    def Contains(self, statement, stdin):
        return statement in stdin.upper()
