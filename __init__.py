import os
from .DataBase import DataBase, DBError
from .ConfigManager import ConfigManager

def initialize_db(config:ConfigManager):
    global DB
    DB = DataBase(config)
    
config = ConfigManager()
DB = DataBase(config)

from .GlobalErrorHandler import GlobalErrorHandler

# current_dir = "D:/Python_scripts/Classes/tests/SerialNames/"
# cnf = ConfigManager(current_dir, current_dir,)
# initialize_db(cnf)

__version__ = '0.1'
