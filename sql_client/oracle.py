# -*- coding: utf-8 -*-

import functools
from typing import Any, Union, Optional, Tuple, Iterable, Collection, Callable, Generator

import cx_Oracle

from .base import SqlClient as BaseSqlClient, Paramstyle, NOTSET, Notset


class SqlClient(BaseSqlClient):
    lib = cx_Oracle

    def __init__(self, host: Optional[str] = None, port: Union[int, str, None] = 1521, user: Optional[str] = None,
                 password: Optional[str] = None, database: Optional[str] = None, charset: Optional[str] = 'utf8',
                 autocommit: bool = True, connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: str = 'INSERT INTO', dictionary: bool = False, escape_auto_format: bool = False,
                 escape_formatter: str = '"{}"', empty_string_to_none: bool = True, args_to_dict: Optional[bool] = None,
                 to_paramstyle: Optional[Paramstyle] = Paramstyle.numeric, try_reconnect: bool = True,
                 try_times_connect: Union[int, float] = 3, time_sleep_connect: Union[int, float] = 3,
                 raise_error: bool = False, exc_info: Optional[bool] = None):
        # oracle如果用双引号escape字段则区分大小写, 故默认escape_auto_format=False
        # oracle无replace语句; insert必须带into
        # 若database为空则host视为tnsname
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

    def connect(self) -> None:
        self.connection = self.lib.connect(user=self.user, password=self.password, dsn='{}:{}/{}'.format(
            self.host, self.port, self.database) if self.database is not None else self.host, encoding=self.charset)
        self.connection.autocommit = self._autocommit
        self.connected = True

    def execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                chunksize: Optional[int] = None, many: bool = False, commit: Optional[bool] = None,
                keep_cursor: Optional[bool] = False, cursor: Optional[cx_Oracle.Cursor] = None
                ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                           Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                                 cx_Oracle.Cursor]]:
        # cx_Oracle.Cursor.execute不能传入None
        # execute执行后修改rowfactory才有效
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        if not many:
            cursor.execute(query, () if args is None else args)
        else:  # executemany: 一句插入多条记录, 当语句超出1024000字符时拆分成多个语句; 传单条记录需用列表包起来
            cursor.executemany(query, args)
        if commit and not self._autocommit:
            self.commit()
        if fetchall and (self.dictionary if dictionary is None else dictionary):
            cursor.rowfactory = lambda *args: dict(zip((col[0] for col in cursor.description), args))
        result = (cursor.fetchall() if chunksize is None else self._fetchmany_generator(cursor, chunksize, keep_cursor)
                  ) if fetchall else len(args) if many and hasattr(args, '__len__') else 1
        if keep_cursor:
            return result, cursor
        if ori_cursor is None and (chunksize is None or not fetchall):
            cursor.close()
        return result

    def format(self, query: str, args: Any, raise_error: Optional[bool] = None,
               cursor: Optional[cx_Oracle.Cursor] = None) -> str:
        # cx_Oracle.Connection没有literal和escape, 暂不借鉴mysql实现
        try:
            if args is None:
                return query
            return query % args if '%' in query else query.format(args)
        except Exception as e:
            if raise_error or raise_error is None and self.raise_error:
                raise e
            return query

    def _before_query_and_get_cursor(self, fetchall: bool = True, dictionary: Optional[bool] = None
                                     ) -> cx_Oracle.Cursor:
        self.set_connection()
        return self.connection.cursor()

    def _callproc(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                  chunksize: Optional[int] = None, many: bool = False, commit: Optional[bool] = None,
                  keep_cursor: Optional[bool] = False, cursor: Optional[cx_Oracle.Cursor] = None,
                  kwargs: Optional[dict] = None
                  ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                             Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                                   cx_Oracle.Cursor]]:
        # cx_Oracle.Cursor.callproc支持kwargs
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        cursor.callproc(query, args, kwargs)
        if commit and not self._autocommit:
            self.commit()
        result = (cursor.fetchall() if chunksize is None else self._fetchmany_generator(cursor, chunksize, keep_cursor)
                  ) if fetchall else len(args) if many and hasattr(args, '__len__') else 1
        if keep_cursor:
            return result, cursor
        if ori_cursor is None and (chunksize is None or not fetchall):
            cursor.close()
        return result

    def call_proc(self, name: str, args: Iterable = (), fetchall: bool = True, dictionary: Optional[bool] = None,
                  chunksize: Optional[int] = None, not_one_by_one: bool = True, auto_format: bool = False,
                  keys: Union[str, Collection[str], None] = None, commit: Optional[bool] = None,
                  escape_auto_format: Optional[bool] = None, escape_formatter: Optional[str] = None,
                  empty_string_to_none: Optional[bool] = None, args_to_dict: Union[bool, Notset, None] = NOTSET,
                  to_paramstyle: Union[Paramstyle, Notset, None] = NOTSET, keep_cursor: Optional[bool] = False,
                  cursor: Optional[cx_Oracle.Cursor] = None, try_times_connect: Union[int, float, None] = None,
                  time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                  exc_info: Union[bool, Notset, None] = NOTSET, call: Optional[Callable] = None,
                  kwargs: Optional[dict] = None
                  ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                             Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                                   cx_Oracle.Cursor]]:
        # cx_Oracle.Cursor.callproc支持kwargs
        # 执行存储过程
        # name: 存储过程名
        # args: 存储过程参数(不能为None, 要可迭代)
        # fetchall=False: return成功执行数(1)
        # keep_cursor: 返回(result, cursor), 并且不自动关闭cursor
        if call is None:
            call = functools.partial(self.try_execute, call=self._callproc, kwargs=kwargs)
        return super().query(name, args, fetchall, dictionary, chunksize, not_one_by_one, auto_format, keys, commit,
                             escape_auto_format, escape_formatter, empty_string_to_none, args_to_dict, to_paramstyle,
                             keep_cursor, cursor, try_times_connect, time_sleep_connect, raise_error, exc_info, call)

    def _callfunc(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                  chunksize: Optional[int] = None, many: bool = False, commit: Optional[bool] = None,
                  keep_cursor: Optional[bool] = False, cursor: Optional[cx_Oracle.Cursor] = None,
                  kwargs: Optional[dict] = None, return_type: type = None
                  ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                             Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                                   cx_Oracle.Cursor]]:
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        cursor.callfunc(query, return_type, args, kwargs)
        if commit and not self._autocommit:
            self.commit()
        result = (cursor.fetchall() if chunksize is None else self._fetchmany_generator(cursor, chunksize, keep_cursor)
                  ) if fetchall else len(args) if many and hasattr(args, '__len__') else 1
        if keep_cursor:
            return result, cursor
        if ori_cursor is None and (chunksize is None or not fetchall):
            cursor.close()
        return result

    def call_func(self, name: str, return_type: type, args: Iterable = (), fetchall: bool = True,
                  dictionary: Optional[bool] = None, chunksize: Optional[int] = None, not_one_by_one: bool = True,
                  auto_format: bool = False, keys: Union[str, Collection[str], None] = None,
                  commit: Optional[bool] = None, escape_auto_format: Optional[bool] = None,
                  escape_formatter: Optional[str] = None, empty_string_to_none: Optional[bool] = None,
                  args_to_dict: Union[bool, Notset, None] = NOTSET,
                  to_paramstyle: Union[Paramstyle, Notset, None] = NOTSET, keep_cursor: Optional[bool] = False,
                  cursor: Optional[cx_Oracle.Cursor] = None, try_times_connect: Union[int, float, None] = None,
                  time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                  exc_info: Union[bool, Notset, None] = NOTSET, call: Optional[Callable] = None,
                  kwargs: Optional[dict] = None
                  ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                             Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                                   cx_Oracle.Cursor]]:
        # 执行函数
        # name: 函数名
        # return_type: 返回值的类型(必填), 参见：
        #              https://cx-oracle.readthedocs.io/en/latest/user_guide/plsql_execution.html#plsqlfunc
        #              https://cx-oracle.readthedocs.io/en/latest/api_manual/cursor.html#Cursor.callfunc
        # args: 函数参数(不能为None, 要可迭代)
        # fetchall=False: return成功执行数(1)
        # keep_cursor: 返回(result, cursor), 并且不自动关闭cursor
        if call is None:
            call = functools.partial(self.try_execute, call=self._callfunc, kwargs=kwargs, return_type=return_type)
        return super().query(name, args, fetchall, dictionary, chunksize, not_one_by_one, auto_format, keys, commit,
                             escape_auto_format, escape_formatter, empty_string_to_none, args_to_dict, to_paramstyle,
                             keep_cursor, cursor, try_times_connect, time_sleep_connect, raise_error, exc_info, call)
