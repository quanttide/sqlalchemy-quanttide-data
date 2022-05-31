# -*- coding: utf-8 -*-

from typing import Any, Union, Optional

import psycopg2.extras
import psycopg2.extensions

from .base import SqlClient as BaseSqlClient, Paramstyle


class SqlClient(BaseSqlClient):
    lib = psycopg2

    def __init__(self, host: Optional[str] = None, port: Union[int, str, None] = 5432, user: Optional[str] = None,
                 password: Optional[str] = None, database: Optional[str] = None, charset: Optional[str] = None,
                 autocommit: bool = True, connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: str = 'INSERT INTO', dictionary: bool = False, escape_auto_format: bool = False,
                 escape_formatter: str = '"{}"', empty_string_to_none: bool = True, args_to_dict: Optional[bool] = None,
                 to_paramstyle: Optional[Paramstyle] = Paramstyle.format, try_reconnect: bool = True,
                 try_times_connect: Union[int, float] = 3, time_sleep_connect: Union[int, float] = 3,
                 raise_error: bool = False, exc_info: Optional[bool] = None):
        # postgresql如果用双引号escape字段则区分大小写, 故默认escape_auto_format=False
        # postgresql无replace语句; insert必须带into
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         args_to_dict, to_paramstyle, try_reconnect, try_times_connect, time_sleep_connect, raise_error,
                         exc_info)

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool):
        if self.connection is not None and value != self._autocommit:
            self.connection.autocommit = value
        self._autocommit = value

    def begin(self) -> None:
        # postgresql库无begin, 只有commit和rollback
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()

    def connect(self) -> None:
        self.connection = self.lib.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.database)
        self.connection.autocommit = self._autocommit
        self.connected = True

    def ping(self) -> None:
        # psycopg2.connection没有ping
        self.set_connection()

    def format(self, query: str, args: Any, raise_error: Optional[bool] = None,
               cursor: Optional[psycopg2.extensions.cursor] = None) -> str:
        # psycopg2.connection没有literal和escape, 但psycopg2.cursor有mogrify
        try:
            if args is None:
                return query
            ori_cursor = cursor
            if cursor is None:
                cursor = self.connection.cursor()
            if isinstance(args, dict):
                new_args = dict((key, cursor.mogrify(item)) for key, item in args.items())
                formatted_query = query % new_args if '%' in query else query.format(**new_args)
            else:
                new_args = tuple(map(cursor.mogrify, args))
                formatted_query = query % new_args if '%' in query else query.format(*new_args)
            if ori_cursor is None:
                cursor.close()
            return formatted_query
        except Exception as e:
            if raise_error or raise_error is None and self.raise_error:
                raise e
            return query

    def _before_query_and_get_cursor(self, fetchall: bool = True, dictionary: Optional[bool] = None
                                     ) -> psycopg2.extensions.cursor:
        if fetchall and (self.dictionary if dictionary is None else dictionary):
            cursor_class = self.lib.extras.DictCursor
        else:
            cursor_class = None
        self.set_connection()
        return self.connection.cursor(cursor_factory=cursor_class)
