import os
from .DataBase import DataBase, DBError
from .ConfigManager import ConfigManager


class DBCallable:
    def __init__(self):
        self._db_instance = None
        self._default_config = ConfigManager()
        self._db_instance = DataBase(self._default_config, is_test=True)

    def __call__(self, config: ConfigManager = None, is_test: bool = None):
        """Возвращает экземпляр БД (тестовую по умолчанию)"""
        if config is None and is_test is None:
            return self._db_instance
        if config is None:
            config = self._default_config
        if is_test is None:
            is_test = self.is_test
        self._db_instance = DataBase(config, is_test=is_test)
        return self._db_instance

    def __getattr__(self, name):
        """Доступ к методам БД по умолчанию"""
        default_db = self()
        return getattr(default_db, name)


DB = DBCallable()
__version__ = '0.1'
__all__ = ["DB", "ConfigManager", "DBError"]
