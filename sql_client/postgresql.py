# -*- coding: utf-8 -*-

import time
from typing import Optional, Union, Any

import psycopg2.extras
import psycopg2.extensions

from .base import SqlClient as BaseSqlClient


class SqlClient(BaseSqlClient):
    lib = psycopg2

    def __init__(self, host: Optional[str] = None, port: Union[int, str, None] = 5432, user: Optional[str] = None,
                 password: Optional[str] = None, database: Optional[str] = None, charset: Optional[str] = None,
                 autocommit: bool = True, connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: str = 'INSERT INTO', dictionary: bool = False, escape_auto_format: bool = False,
                 escape_formatter: str = '"{}"', empty_string_to_none: bool = True, keep_args_as_dict: bool = True,
                 transform_formatter: bool = True, try_times_connect: Union[int, float] = 3,
                 time_sleep_connect: Union[int, float] = 3, raise_error: bool = False):
        # postgresql如果用双引号escape字段则区分大小写，故默认escape_auto_format=False
        # postgresql无replace语句；insert必须带into
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         keep_args_as_dict, transform_formatter, try_times_connect, time_sleep_connect, raise_error)

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool):
        if hasattr(self, 'connection') and value != self._autocommit:
            self.connection.autocommit = value
        self._autocommit = value

    def begin(self) -> None:
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()

    def connect(self) -> None:
        self.connection = self.lib.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.database)
        self.connection.autocommit = self._autocommit

    def try_execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                    many: bool = False, commit: Optional[bool] = None,
                    try_times_connect: Union[int, float, None] = None,
                    time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                    cursor: Any = None) -> Union[int, tuple, list]:
        # psycopg2.connection没有literal和escape，但psycopg2.cursor有mogrify
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        if try_times_connect is None:
            try_times_connect = self.try_times_connect
        if time_sleep_connect is None:
            time_sleep_connect = self.time_sleep_connect
        if raise_error is None:
            raise_error = self.raise_error
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        try_count_connect = 0
        while True:
            try:
                result = self.execute(query, args, fetchall, dictionary, many, commit, cursor)
                if ori_cursor is None:
                    cursor.close()
                return result
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  {}'.format(
                            str(type(e))[8:-2], try_count_connect, e, self._query_log_text(query, args, cursor)),
                            exc_info=True)
                    if raise_error:
                        if ori_cursor is None:
                            cursor.close()
                        raise e
                    break
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  {}'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e,
                        self._query_log_text(query, args, cursor)), exc_info=True)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                self.rollback()
                if self.log:
                    self.logger.error('{}: {}  {}'.format(
                        str(type(e))[8:-2], e, self._query_log_text(query, args, cursor)), exc_info=True)
                if raise_error:
                    if ori_cursor is None:
                        cursor.close()
                    raise e
                break
        if fetchall:
            return ()
        return 0

    def ping(self) -> None:
        # psycopg2.connection没有ping
        self.set_connection()

    def format(self, query: str, args: Any, raise_error: Optional[bool] = None, cursor: Any = None) -> str:
        # psycopg2.connection没有literal和escape，但psycopg2.cursor有mogrify
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

    def _query_log_text(self, query: str, args: Any, cursor: Any = None) -> str:
        try:
            return 'formatted_query: {}'.format(self.format(query, args, True, cursor))
        except Exception as e:
            return 'query: {}  args: {}  {}: {}'.format(query, args, str(type(e))[8:-2], e)
