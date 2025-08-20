import os
import configparser
from threading import Lock

FOLDER_PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FOLDER_FILES = os.path.join(FOLDER_PROJECT,"DataBase")

class ConfigManager:
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls, 
                folder=FOLDER_FILES,
                folder_db=FOLDER_FILES,
                filename='config.ini'):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls.folder = folder
                cls.folder_db = folder_db
                cls.filename = os.path.join(folder, filename)
                cls.config = configparser.ConfigParser()
        return cls._instance

    def __init__(self,
                 folder=FOLDER_FILES,
                 folder_db=FOLDER_FILES,
                 filename='config.ini'):
        with self._lock:
            if not os.path.exists(self.filename):
                self.createConfig()
            else:
                self.loadConfig()

    def createConfig(self):
        """Создает конфигурационный файл по умолчанию."""
        archive_folder = r"C:\Users\Public\AppData\Integral-T"
        self.config['DB'] = {
            'dbFolder': self.folder_db,
            'archiveFolder': archive_folder, 
            'dbName': "database.db3",
        }
        os.makedirs(self.folder_db, exist_ok=True)
        os.makedirs(archive_folder, exist_ok=True)
        self.saveConfig()

    def loadConfig(self):
        """Загружает конфигурационный файл."""
        self.config.read(self.filename)

    def saveConfig(self):
        """Сохраняет текущие настройки в конфигурационный файл."""
        with open(self.filename, 'w') as configfile:
            self.config.write(configfile)

    def getDbFolder(self):
        """Возвращает путь к папке базы данных."""
        return self.config['DB'].get('dbFolder', '')
    
    def getArchivesFolder(self):
        """Возвращает путь к папке c архивами БД."""
        return self.config['DB'].get('archiveFolder', '')
    
    def getDbCurrent(self):
        """Возвращает путь к текущей базе данных."""
        return self.config['DB'].get('dbName', '')

    def setDbFolder(self, folder_path):
        """Устанавливает путь к папке базы данных."""
        self.config['DB']['dbFolder'] = folder_path
        os.makedirs(folder_path, exist_ok=True)
        self.saveConfig()
        
    def setArchivesFolder(self, folder_path):
        """Устанавливает путь к папке базы данных."""
        self.config['DB']['archiveFolder'] = folder_path
        os.makedirs(folder_path, exist_ok=True)
        self.saveConfig()

    def setDbCurrent(self, db_path):
        """Устанавливает путь к текущей базе данных."""
        self.config['DB']['dbName'] = db_path
        self.saveConfig()
        
if __name__ == "__main__":
    config = ConfigManager()