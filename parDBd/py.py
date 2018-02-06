import socket;
import sqlite3;
import threading

SETTINGCFG = 'server_settings.txt'
SETTINGSCFG_DEFAULT = """PORT=5506
HOST=127.0.0.1
"""
SETTINGS = []

#VERSION = '1.0.1'

#create the sqlite instance
SQLITE3_CONN = sqlite3.connect('parDBd.db');

#based on example from 
#https://docs.python.org/2/library/sqlite3.html
def Main():
    print("Starting parDBd Server.");
    global SQLITE3_CONN
    
    try:
        READ_Settings(SETTINGCFG)
    except OSError:
        print("OSError: Could not find a configuration file! Creating one...")
        INIT_Settings(SETTINGCFG)
    except IOError:
        print("IOError: Could not find a configuration file! Creating one...")
        INIT_Settings(SETTINGCFG)
    
    try:
        #thread1 = threading.Thread(target = SOCKET_LISTEN())
        #thread2 = threading.Thread(target = CONSOLE())
        #thread2.start()
        #thread1.start()
        
        
        #thread2.join()
        #thread1.join()
        SOCKET_LISTEN()
    except KeyboardInterrupt:
        print("Keyboard interruption detected, stopping server...")
    
    SQLITE3_CONN.commit()
    SQLITE3_CONN.close()
    
    print("Server stopped.")

def CONSOLE():
    while True:
        command = input(': ')

#based on example from 
#https://shakeelosmani.wordpress.com/2015/04/13/python-3-socket-programming-example/
def SOCKET_LISTEN():
    print("Accepting connections. Listening for queries...");
    ServerSocket = socket.socket();
    ServerSocket.bind((SETTINGS_Get('HOST'), int(SETTINGS_Get('PORT'))));
    while True: #initial listen loop
        ServerSocket.listen(1);
    
        ConnectionInstance, addr = ServerSocket.accept();
        print ("Connection from: " + str(addr))
        while True: #todo later instantiate as different threads
            global SQLITE3_CONN
            data = ConnectionInstance.recv(1024).decode()
            if not data:
                break
            print ("The connected user sent the following queries:\n" + str(data))
            
            cursor = SQLITE3_CONN.cursor()
            msg = ''
            try:
                cursor.execute(str(data))
                SQLITE3_CONN.commit()
                data = str(data)
                msg = ('[SERVER]: Processing the following queries:\n' + data).encode()
            except sqlite3.OperationalError as e:
                msg = ('[SERVER]: Error processing queries: ' + str(e)).encode()
            print(msg)
            ConnectionInstance.send(msg)
    
def READ_Settings(file):
    print("Reading server configuration file...")
    settings = open(file, 'r')
    for line in settings:
        if line[0] != '#' and '=' in line:
            key, val = line.split('=')
            val = val.strip('\n\r')
            if SETTINGS_Contains(key) is True:
                for KVPAIR in SETTINGS:
                    if KVPAIR.key == key:
                        KVPAIR.val = val
            else:
                SETTING = KVPair()
                SETTING.key = key
                SETTING.val = val
                SETTINGS.append(SETTING)
    settings.close()
    print('Loaded the following settings:')
    SETTINGS_PrintSettings()
    
def INIT_Settings(file):
    print("Creating default server configuration file...")
    settings = open(file, 'w+')
    settings.write(SETTINGSCFG_DEFAULT)
    settings.close()
    
class KVPair(object):
    key = ""
    val = ""
    
    def __init__(self):
        self.key = ""
        self.val = ""
    
    def ToString(self):
        return (self.key + '=' + self.val)
    
def SETTINGS_Contains(key):
    for KVPair in SETTINGS:
        if (KVPair.key == key):
            return True
    return False

def SETTINGS_PrintSettings():
    for KVPair in SETTINGS:
        print(KVPair.ToString())
        
def SETTINGS_Get(key):
    for KVPair in SETTINGS:
        if (KVPair.key == key):
            return KVPair.val
    return ''
    
Main()