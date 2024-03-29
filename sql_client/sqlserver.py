# -*- coding: utf-8 -*-

from typing import Any, Union, Optional

import pymssql

from .base import SqlClient as BaseSqlClient, Paramstyle


class SqlClient(BaseSqlClient):
    lib = pymssql

    def __init__(self, host: Optional[str] = None, port: Union[int, str, None] = 1433, user: Optional[str] = None,
                 password: Optional[str] = None, database: Optional[str] = None, charset: Optional[str] = 'utf8',
                 autocommit: bool = True, connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: str = 'INSERT', dictionary: bool = False, escape_auto_format: bool = True,
                 escape_formatter: str = '[{}]', empty_string_to_none: bool = True, args_to_dict: Optional[bool] = None,
                 to_paramstyle: Optional[Paramstyle] = Paramstyle.format, try_reconnect: bool = True,
                 try_times_connect: Union[int, float] = 3, time_sleep_connect: Union[int, float] = 3,
                 raise_error: bool = False, exc_info: Optional[bool] = None):
        # sqlserver无replace语句
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         args_to_dict, to_paramstyle, try_reconnect, try_times_connect, time_sleep_connect, raise_error,
                         exc_info)

    def begin(self) -> None:
        # sqlserver库无begin, 只有commit和rollback
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()

    def connect(self) -> None:
        self.connection = self.lib.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.database, charset=self.charset, autocommit=self._autocommit,
                                           as_dict=self.dictionary)
        self.connected = True

    def ping(self) -> None:
        # pymssql.Connection没有ping
        self.set_connection()

    def format(self, query: str, args: Any, raise_error: Optional[bool] = None, cursor: Optional[pymssql.Cursor] = None
               ) -> str:
        # pymssql.Connection没有literal和escape, 暂不借鉴mysql实现
        try:
            if args is None:
                return query
            return query % args if '%' in query else query.format(args)
        except Exception as e:
            if raise_error or raise_error is None and self.raise_error:
                raise e
            return query

    def _before_query_and_get_cursor(self, fetchall: bool = True, dictionary: Optional[bool] = None) -> pymssql.Cursor:
        if fetchall and dictionary is not None and dictionary != self.dictionary:
            if hasattr(self, 'connection'):
                self.connection.as_dict = dictionary
            self.dictionary = dictionary
        self.set_connection()
        return self.connection.cursor()
