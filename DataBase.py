"""Класс для работы с БД"""

from datetime import datetime
from .DataBaseManager import DataBaseManager
from .ConfigManager import ConfigManager
from utils import FORMAT_DATE

from enum import Enum
from typing import List, Callable, Tuple, Dict, Any


class DBError(Exception):
    """Исключение для ошибок, связанных с базой данных."""

    def __init__(self, message, sql_error: Exception = None):
        """
        Инициализация исключения.

        Parameters
        ----------
        txt : str
            Сообщение об ошибке.
        sql_error : Exception
            Ошибка SQL, если есть (по умолчанию None).
        """
        super().__init__(message)
        self.sql_error = sql_error


class DataBase(DataBaseManager):
    """ Класс для работы с БД"""
    _tables = {}

    class Column:
        """Класс для представления информации о колонке таблицы"""

        class Type(Enum):
            INTEGER = int
            REAL = float
            TEXT = str
            BLOB = bytes

            @classmethod
            def get(cls, name: str):
                for t in cls:
                    if t.name == name:
                        return t
                raise ValueError(name)

        def __init__(self, *args):
            self.index, self.name, col_type, notnull, dflt_value, \
                self.pk = args   
            self.col_type = self.Type.get(col_type)
            self.notnull = bool(notnull)
            self.dflt_value = None
            if dflt_value is not None:
                if self.col_type == self.Type.REAL:
                    self.dflt_value = float(dflt_value)
                elif self.col_type == self.Type.INTEGER:
                    self.dflt_value = int(dflt_value)

        def convert(self, value: Any) -> Any:
            if value is None: #and not self.notnull:
                return None
            return self.col_type.value(value)


    def __init__(self, config: ConfigManager):
        super().__init__(config)
        self._tables = {table: self._getColumns(table)
                        for table in self.tables}

    @staticmethod
    def j1(args) -> str:
        """Создание строки из элементов списка с разделителем-запятой.

        Пример:
            j1("id", "name", "date") → "id, name, date"
        """
        return ", ".join(args)

    @staticmethod
    def j2(args) -> str:
        """Создание строки с `?` для параметризованных SQL-запросов.

        Пример:
            j2("id", "name", "date") → "?, ?, ?"
        """
        return ", ".join(["?"] * len(args))

    @staticmethod
    def j3(args, word="AND") -> str:
        """Генерация SQL-условия вида `key1 = ? AND key2 = ? ...`.

        Пример:
            j3("id", "name", word="OR") → "id = ? OR name = ?"
        """
        return f" = ? {word} ".join(args) + " = ?"

    @classmethod
    def _get_func(cls, operator: Callable) -> Callable[..., List[str]]:
        """Возвращает функцию для генерации SQL-условия в зависимости от оператора.

        Пример:
            get_func(all) → возвращает функцию, которая генерирует условия с AND
            get_func(any) → возвращает функцию, которая генерирует условия с OR
        """
        if operator == all:
            return lambda fields: cls.j3(fields, word="AND")
        elif operator == any:
            return lambda fields: cls.j3(fields, word="OR")
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def getTimeLastUpdate(self) -> datetime:
        txt = "SELECT MAX(date_update) FROM links"
        with self as cursor:
            cursor.execute(txt)
            value = cursor.fetchone()[0]
            try:
                dt = datetime.fromtimestamp(int(value))
            except ValueError:
                dt = datetime.strptime(value, FORMAT_DATE)
            return dt

    @property
    def tables(self) -> Tuple[str]:
        """Получение имен таблиц БД"""
        txt = "SELECT name FROM sqlite_master WHERE type IN ('table', 'view');"
        with self as cursor:
            cursor.execute(txt)
            return tuple(t[0] for t in cursor.fetchall())

    def _getColumns(self, table: str) -> Tuple[Column, ...]:
        """Получение названия колонок таблицы table"""
        txt = f"PRAGMA table_info({table})"
        with self as cursor:
            cursor.execute(txt)
            columns = cursor.fetchall()
            processed_columns = []
            for col in columns:
                column_obj = self.Column(*col)
                processed_columns.append(column_obj)
        return dict((col.name, col) for col in processed_columns)

    def getColumnsNames(self, table: str) -> Tuple[str]:
        """Получение названия колонок таблицы table"""
        if table not in self._tables:
            raise DBError(f"Несуществующая таблица {table}")
        return tuple(col for col in self._tables[table])

    def getColumns(self, table: str) -> Dict[str, Column]:
        if table not in self._tables:
            raise DBError(f"Несуществующая таблица {table}")
        return self._tables[table]

    def makeRequest(self, txt: str, *args) -> List:
        """
        Выполнение SQL запроса к БД

        Parameters
        ----------
        txt : str
            Текст SQL запроса.
        *args
            Аргументы для SQL запроса.

        Returns
        -------
        list
            Результат выполнения запроса.
        """
        with self as cursor:
            if args:
                cursor.execute(txt, args)
            else:
                cursor.execute(txt)
            return cursor.fetchall()

    def getRowByValue(self,
                      value: any,
                      table: str,
                      key: str = "name") -> Dict[str, Any]:
        """
        Получение значений из таблицы по заданному ключу и значению

        Parameters
        ----------
        value : any
            Значение ключа для поиска.
        table : str
            Название таблицы.
        key : str, optional
            Ключ для поиска, по умолчанию "name".

        Returns
        -------
        dict
            Значения из таблицы в виде словаря.
        """
        txt = f'SELECT * FROM {table} WHERE {key} = ?'
        with self as cursor:
            cursor.execute(txt, (value,))
            values = cursor.fetchone()
            columns = list(map(lambda x: x[0], cursor.description))
            if not values:
                return {}
                raise DBError(f"Не найден в таблице {table} в колонке "
                              f"{key} значение {value}")
            return dict(zip(columns, values))

    def getIDbyValue(self,
                     value: any,
                     table: str,
                     key: str = "name") -> int or None:
        """
        Получение ID из таблицы по заданному ключу и значению

        Parameters
        ----------
        value : any
            Значение ключа для поиска.
        table : str
            Название таблицы.
        key : str, optional
            Ключ для поиска, по умолчанию "name".

        Returns
        -------
        int or None
            ID найденной записи или None, если запись не найдена.
        """
        with self as cursor:
            cursor.execute(f'SELECT id FROM {table} WHERE {key} = ?', (value,))
            id_ = cursor.fetchone()
        if id_ is None:
            return None
        return id_[0]

    def getRowsbyColumn(self,
                        table: str,
                        key: str = "name") -> list:
        """
        Получение строк и их идентификаторов из таблицы

        Parameters
        ----------
        table : str
            Название таблицы.
        key : str, optional
            Ключ для поиска, по умолчанию "name".

        Returns
        -------
        list
            Список строк и их идентификаторов.
        """
        with self as cursor:
            cursor.execute(f"SELECT id,{key} FROM {table}")
            l = [i[0:2] for i in cursor.fetchall()]
        return l

    def getIdsFromView(self, table: str, links: list, tags: set) -> list:
        links = [l["id"] for l in links]
        tag_exists = any(col == 'tag' for col in self.getColumns(table))
        with self as cursor:
            cursor.execute(f"DROP VIEW IF EXISTS view_{table};")
            txt = f"""
            CREATE VIEW view_{table} AS
            SELECT links.id, links.id_child
            FROM links
            """
            if tag_exists:
                ids = [f"{table}.tag = {tag.id}" for tag in tags]
                txt += f"""INNER JOIN {table} ON links.id_child = {table}.id
                AND ({' OR '.join(ids)})
                """
            cursor.execute(txt)
        with self as cursor:
            cursor.execute(f"SELECT * FROM view_{table}")
            rows = cursor.fetchall()
            rows = list(filter(lambda r: r[0] in links, rows))
            return rows

    def getValueByValues(self, table: str,
                         column: str = "name",
                         operator: Callable = all,
                         **kwargs) -> any:
        """
        Получение значения из таблицы по заданным ключам и значениям

        Parameters
        ----------
        table : str
            Название таблицы.
        tag : str, optional
            Тег для поиска, по умолчанию "name".
        **kwargs
            Ключи и значения для поиска.

        Returns
        -------
        any or None
            Найденное значение из таблицы или None, если запись не найдена.
        """
        join_func = self._get_func(operator)
        if not kwargs:
            raise AttributeError("Не заданы значения для поиска")
        with self as cursor:
            txt = f'SELECT {column} FROM {table} WHERE '\
                f'{join_func(list(kwargs.keys()))}'
            cursor.execute(txt, list(kwargs.values()))
            values = cursor.fetchone()
            if values:
                return values[0]
        return None

    def getRowByValues(self, table: str,
                       operator: Callable = all,
                       **kwargs) -> any:
        """
        Получение значения из таблицы по заданным ключам и значениям

        Parameters
        ----------
        table : str
            Название таблицы.
        tag : str, optional
            Тег для возвращения, по умолчанию "name".
        **kwargs
            Ключи и значения для поиска.

        Returns
        -------
        any or None
            Найденное значение из таблицы или None, если запись не найдена.
        """
        join_func = self._get_func(operator)
        columns = self.getColumnsNames(table)
        with self as cursor:
            txt = f'SELECT * FROM {table} WHERE '\
                f'{join_func(list(kwargs.keys()))}'
            cursor.execute(txt, list(kwargs.values()))
            values = cursor.fetchone()
            if values:
                return dict(zip(columns, values))
        return {}

    def getValueByValue(self,
                        columnIn: str,
                        value: any,
                        columnOut: str,
                        table: str) -> tuple:
        """
        Получение значения из таблицы по значению в одном столбце

        Parameters
        ----------
        columnIn : str
            Имя входного столбца.
        value : any
            Значение для поиска.
        columnOut : str
            Имя столбца, значение которого нужно вернуть.
        table : str
            Название таблицы.

        Returns
        -------
        any
            Найденное значение из таблицы.
        """
        txt = f'SELECT {columnOut} FROM {table} WHERE {columnIn} = ?'
        with self as cursor:
            cursor.execute(txt, (value,))
            v = cursor.fetchall()
        if len(v) == 0:
            return None
        return v[0][0]

    def getValueById(self, id_: int,
                     table: str,
                     key: str = "name") -> any:
        """
        Получение значения из таблицы по ID

        Parameters
        ----------
        id : int
            Идентификатор записи.
        table : str
            Название таблицы.
        key : str, optional
            Ключ для поиска, по умолчанию "name".

        Returns
        -------
        any
            Найденное значение из таблицы.
        """
        with self as cursor:
            txt = f'SELECT {key} FROM {table} WHERE id = ?'
            cursor.execute(txt, (id_,))
            value = cursor.fetchone()
        return value[0]

    def getRowsbyValues(self, table: str,
                        operator=all,
                        **kwargs) -> List[dict]:
        """
        Получение всех строк из таблицы по заданным ключам и значениям

        Parameters
        ----------
        table : str
            Название таблицы.
        **kwargs
            Ключи и значения для поиска.

        Returns
        -------
        list
            Список словарей с найденными значениями из таблицы.
        """
        join_func = self._get_func(operator)
        txt = f'SELECT * FROM {table} WHERE {join_func(list(kwargs.keys()))}'
        with self as cursor:
            cursor.execute(txt, list(kwargs.values()))
            keys = list(map(lambda x: x[0], cursor.description))
            values = cursor.fetchall()
        return list(map(lambda value: dict(zip(keys, value)), values))

    # def getConnectsItems(self, id_link:int) -> List[list]:
    #     """
    #     Получение связанных элементов

    #     Parameters
    #     ----------
    #     id_link : int
    #         Идентификатор ссылки.

    #     Returns
    #     -------
    #     list
    #         Список связанных элементов.
    #     """
    #     txt = f"SELECT * FROM {CONNECTION} WHERE (id_A = ? OR id_B = ?)"
    #     values = [id_link] * 2
    #     connects = self.makeRequest(txt, *values)
    #     connects_new = []
    #     for con in connects:
    #         con = list(con)
    #         i = con.index(id_link, 1, -1)
    #         con.pop(i)
    #         connects_new.append(con[:3])
    #     return connects_new

    def filterKwargs(self, kwargs):
        for k, v in kwargs.copy().items():
            if v is None:
                del kwargs[k]

    def insertObject(self,
                     table: str,
                     autocommit: bool,
                     **kwargs) -> int:
        """
        Вставка объекта в таблицу

        Parameters
        ----------
        table : str
            Название таблицы.
        **kwargs
            Параметры объекта для вставки.

        Returns
        -------
        int
            Идентификатор вставленной записи.
        """

        id_ = kwargs.pop("id") if "id" in list(kwargs.keys()) else None
        self.filterKwargs(kwargs)
        self._autocommit = autocommit
        if id_:
            cut = self.j1([f"{k}='{v}'" for k, v in kwargs.items()])
            txt = f'UPDATE {table} SET {cut} WHERE id = {id_}'
            with self as cursor:
                cursor.execute(txt,)
            return id_
        else:
            columns = list(kwargs.keys())
            values = list(kwargs.values())
            if not columns:
                raise AttributeError("Не указаны аргументы")
            txt = f'INSERT INTO {table} ({self.j1(columns)}) VALUES '\
                f'({self.j2(columns)})'
            with self as cursor:
                cursor.execute(txt, values)
            return cursor.lastrowid

    def deleteById(self, table: str, id_: int) -> None:
        """
        Удаление записи из таблицы по идентификатору

        Parameters
        ----------
        table : str
            Название таблицы.
        id_ : int
            Идентификатор записи для удаления.
        """
        txt = f"DELETE FROM {table} WHERE id = ?"
        with self as cursor:
            cursor.execute(txt, (id_,))
