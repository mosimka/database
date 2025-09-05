"""Модуль для подключения к БД"""
import sys
import sqlite3
import os
from typing import Type


from .ConfigManager import ConfigManager

SQL_CREATOR = 'database_creator.sql'


class DataBaseManager:
    """Класс для управления соединением с базой данных SQLite."""

    _config = None
    _instance = None
    _is_test = None
    _autocommit: bool = True
    _connection = None
    _cursor: sqlite3.Cursor

    def __new__(cls, config: ConfigManager, is_test: bool = True):
        if cls._instance is None or \
            config != cls._config or \
                cls._is_test != is_test:
            cls._instance = super().__new__(cls)
            cls.is_test = is_test
            cls.config = config
        print("database:", cls._instance.fullpath)
        return cls._instance

    def __init__(self, config: ConfigManager, is_test: bool = True):
        if not os.path.exists(self.fullpath) or is_test:
            self.createDB()

    def _setup_global_error_handler(self):
        """Перехватывает все необработанные исключения в программе."""
        def handle_exception(exc_type: Type[BaseException], exc_value: BaseException, traceback):
            self.close()  # Закрываем соединение при ошибке
            # Стандартный вывод ошибки
            sys.__excepthook__(exc_type, exc_value, traceback)
        sys.excepthook = handle_exception

    @property
    def folder(self) -> str:
        return self.config.getDbFolder(self.is_test)

    @property
    def filename(self) -> str:
        return self.config.getDbCurrent(self.is_test)

    @property
    def fullpath(self) -> str:
        return os.path.join(self.folder, self.filename)

    def __enter__(self) -> sqlite3.Cursor:
        """Открытие соединения с базой данных при входе в контекстный блок."""
        if self._connection is None:
            self._connection = sqlite3.connect(self.fullpath)
        self._cursor = self._connection.cursor()
        return self._cursor

    def __exit__(self, type_, value, traceback) -> None:
        """Закрытие соединения с базой данных при выходе из контекстного блока."""
        if type_ is not None:
            self.rollback()
        elif self._autocommit:
            self.commit()

    def rollback(self) -> None:
        if self._connection is not None:
            self._connection.rollback()
            self.close()

    def commit(self) -> None:
        if self._connection is not None:
            self._connection.commit()
            self.close()

    def close(self) -> None:
        """Явное закрытие соединения, если оно открыто."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __del__(self):
        self.close()

    def createDB(self) -> None:
        filename = os.path.join(os.path.dirname(self.config.folder),
                                'sql_request')
        filename = os.path.join(filename, SQL_CREATOR)
        with open(filename, 'r', encoding='utf-8') as file:
            sql_script = file.read()
            with self as cursor:
                cursor.executescript(sql_script)
