'''
Created on Feb 13, 2018

@author: Chunmeista
'''
import sqlite3
import socket
import threading
from multiprocessing import Process, Queue
import pickle

from Settings import Settings
from KVPair import KVPair


SETTINGCFG_PATH = './server_settings.txt'
DATABASE_PATH = './parDBd.db'
global SQLITE3_CONN
global SETTINGS


def Main():
    print("Starting parDBd Server.")
    global SQLITE3_CONN
    SQLITE3_CONN = sqlite3.connect(DATABASE_PATH);

    global SETTINGS
    SETTINGS = Settings(SETTINGCFG_PATH)
    
    try:
        jobs = []

        listen_thread = Process(target=SOCKET_LISTEN())
        listen_thread.daemon = True
        #listen_thread.start()
        #terminal_thread = Process(target=CONSOLE())
        #terminal_thread.daemon = True
        #terminal_thread.start()
        #threading._start_new_thread(SOCKET_LISTEN())
        #CONSOLE() #TODO multithreaded console


    except KeyboardInterrupt:
        print("Keyboard interruption detected, stopping server...")
    
    SQLITE3_CONN.commit()
    SQLITE3_CONN.close()
    
    print("Server stopped.")

def CHECK_CONNECTION():
    print()
    #if connection refused
    #else

# based on example from
# https://shakeelosmani.wordpress.com/2015/04/13/python-3-socket-programming-example/
def SOCKET_LISTEN():
    print("Accepting connections. Listening for queries...")
    ServerSocket = socket.socket()
    ServerSocket.bind((SETTINGS.SETTINGS_Get('HOST'), int(SETTINGS.SETTINGS_Get('PORT'))))
    while True:  # initial listen loop

        ServerSocket.listen(1)
        ConnectionInstance, addr = ServerSocket.accept()


        #while True:  # todo later instantiate as different threads
        global SQLITE3_CONN

        received_data = pickle.loads(ConnectionInstance.recv(1024))

        clientcode = str(received_data[0])
        clientdata = str(received_data[1])
        if not clientdata or not clientcode:
            break

        print("Connection from client: " + str(addr[0]) + ":" + str(addr[1]) + ' sent the following:')
        print('CODE: ' + clientcode)
        print('DATA: ' + clientdata)
        print()

        cursor = SQLITE3_CONN.cursor()
        servdata = ''
        servcode = ''
        try:
            if clientcode == '001':
                servcode = '200'
                servdata = 'connection_successful'
            else:
                clientdata = str(clientdata)
                clientcode = str(clientcode)
                #if data sent is connect check
                #else if query is equal to
                cursor.execute(str(clientdata))
                SQLITE3_CONN.commit()

                servcode = '202'
                servdata = 'queries_successful'
        except sqlite3.OperationalError as e:
            servcode = '501'
            servdata = 'operational_error: ' + str(e)
        except sqlite3.IntegrityError as e:
            servcode = '502'
            servdata = 'integrity_error: ' + str(e)
        except TypeError as e:
            servcode = '503'
            servdata = 'type_error: ' + str(e)
        #print(servcode)
        #print(servdata)

        print()

        ConnectionInstance.send(pickle.dumps((servcode, servdata), protocol=2))

def CONSOLE():
    while True:
        command = input(': ')
        print(command)

Main()