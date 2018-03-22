from KVPair import KVPair
import os

class Settings(object):
    global SETTINGS
    SETTINGS = []
    global SETTINGCFG_PATH
    SETTINGCFG_PATH = ""
    global SETTINGCFG_DEFAULT
    SETTINGSCFG_DEFAULT = """PORT=5506
HOST=127.0.0.1
"""

    def __init__(self, settingspath):
        print("Setting Manager initiated.")
        self.SETTINGS = []
        self.SETTINGCFG_PATH = settingspath

        try:
            self.SETTINGS_Load()
        except OSError:
            print("OSError: Could not find a configuration file! Creating one...")
            self.SETTINGS_New()
        except IOError:
            print("IOError: Could not find a configuration file! Creating one...")
            self.SETTINGS_New()

    def SETTINGS_Load(self):
        print("Reading server configuration file...")
        settingsfile = open(self.SETTINGCFG_PATH, 'r')
        print(os.path.realpath(settingsfile.name))
        for line in settingsfile:
            if line[0] != '#' and '=' in line:
                key, val = line.split('=')
                val = val.strip('\n\r')
                if self.SETTINGS_Contains(key) is True:
                    for KVPAIR in self.SETTINGS:
                        if KVPAIR.key == key:
                            KVPAIR.val = val
                else:
                    SETTING = KVPair()
                    SETTING.key = key
                    SETTING.val = val
                    self.SETTINGS.append(SETTING)
        settingsfile.close()
        print('Loaded the following settings:')
        self.SETTINGS_PrintSettings()

    def SETTINGS_New(self):
        print("Creating default server configuration file...")
        settings = open(SETTINGCFG_PATH, 'w+')
        settings.write(self.SETTINGCFG_DEFAULT)
        settings.close()

    def SETTINGS_Contains(self, key):
        for KVPair in self.SETTINGS:
            if KVPair.key == key:
                return True
        return False

    def SETTINGS_PrintSettings(self):
        for KVPair in self.SETTINGS:
            print(KVPair.ToString())
        
    def SETTINGS_Get(self, key):
        for KVPair in self.SETTINGS:
            if KVPair.key == key:
                return KVPair.val
        return ''
