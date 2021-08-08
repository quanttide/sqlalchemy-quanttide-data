# -*- coding: utf-8 -*-

import time
import re
from typing import Optional, Union, Iterable, Collection, Any

import cx_Oracle

from .base import SqlClient as BaseSqlClient


class SqlClient(BaseSqlClient):
    lib = cx_Oracle

    def __init__(self, host: Optional[str] = None, port: Union[int, str, None] = 1521, user: Optional[str] = None,
                 password: Optional[str] = None, database: Optional[str] = None, charset: Optional[str] = 'utf8',
                 autocommit: bool = True, connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: str = 'INSERT INTO', dictionary: bool = False, escape_auto_format: bool = False,
                 escape_formatter: str = '"{}"', empty_string_to_none: bool = True, keep_args_as_dict: bool = True,
                 transform_formatter: bool = True, try_times_connect: Union[int, float] = 3,
                 time_sleep_connect: Union[int, float] = 3, raise_error: bool = False):
        # oracle如果用双引号escape字段则区分大小写，故默认escape_auto_format=False
        # oracle无replace语句；insert必须带into
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         keep_args_as_dict, transform_formatter, try_times_connect, time_sleep_connect, raise_error)

    def query(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
              not_one_by_one: bool = True, auto_format: bool = False, keys: Union[str, Collection[str], None] = None,
              commit: Optional[bool] = None, escape_auto_format: Optional[bool] = None,
              escape_formatter: Optional[str] = None, empty_string_to_none: Optional[bool] = None,
              keep_args_as_dict: Optional[bool] = None, transform_formatter: Optional[bool] = None,
              try_times_connect: Union[int, float, None] = None, time_sleep_connect: Union[int, float, None] = None,
              raise_error: Optional[bool] = None) -> Union[int, tuple, list]:
        # cx_Oracle.Cursor.execute不支持%s，但支持:1, :2, ...
        # args 支持单条记录: list/tuple/dict 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True或keys不为None: 注意此时query会被format一次；keep_args_as_dict强制视为False；
        #                                首条记录需为dict（not_one_by_one=False时所有记录均需为dict），或者含除自增字段外所有字段并按顺序排好各字段值，或者自行传入keys
        # fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        args, is_multiple = self.standardize_args(args, None, empty_string_to_none,
                                                  not auto_format and keys is None and keep_args_as_dict, True)
        if not args:
            return self.try_execute(query, args, fetchall, dictionary, False, commit, try_times_connect,
                                    time_sleep_connect, raise_error)
        if escape_auto_format is None:
            escape_auto_format = self.escape_auto_format
        if not is_multiple or not_one_by_one:  # 执行一次
            if auto_format or keys is not None:
                if escape_formatter is None:
                    escape_formatter = self.escape_formatter
                if keys is None:
                    arg = args[0] if is_multiple else args
                    query = query.format('({})'.format(','.join((escape_formatter.format(
                        key) for key in arg) if escape_auto_format else map(str, arg))) if isinstance(
                        arg, dict) else '', ','.join(':{}'.format(i) for i in range(1, len(args) + 1)))
                elif isinstance(keys, str):
                    query = query.format('({})'.format(','.join(escape_formatter.format(key.strip()) for key in
                                                                keys.split(',')) if escape_auto_format else keys),
                                         ','.join(':{}'.format(i) for i in range(1, keys.count(',') + 2)))
                else:
                    query = query.format('({})'.format(','.join((escape_formatter.format(key) for key in keys)
                                                                if escape_auto_format else keys)),
                                         ','.join(':{}'.format(i) for i in range(1, len(keys) + 1)))
            return self.try_execute(query, args, fetchall, dictionary, is_multiple, commit, try_times_connect,
                                    time_sleep_connect, raise_error)
        # 依次执行
        ori_query = query
        result = [] if fetchall else 0
        if auto_format or keys is not None:
            if escape_formatter is None:
                escape_formatter = self.escape_formatter
            if keys is not None:
                if isinstance(keys, str):
                    query = query.format('({})'.format(','.join(escape_formatter.format(key.strip()) for key in
                                                                keys.split(',')) if escape_auto_format else keys),
                                         ','.join(':{}'.format(i) for i in range(1, keys.count(',') + 2)))
                else:
                    query = query.format('({})'.format(','.join((escape_formatter.format(key) for key in keys)
                                                                if escape_auto_format else keys)),
                                         ','.join(':{}'.format(i) for i in range(1, len(keys) + 1)))
        cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        for arg in args:
            if auto_format and keys is None:
                query = ori_query.format('({})'.format(','.join((escape_formatter.format(
                    key) for key in arg) if escape_auto_format else map(str, arg))) if isinstance(
                    arg, dict) else '', ','.join(':{}'.format(i) for i in range(1, len(args) + 1)))
            temp_result = self.try_execute(query, arg, fetchall, dictionary, not_one_by_one, commit, try_times_connect,
                                           time_sleep_connect, raise_error, cursor)
            if fetchall:
                result.append(temp_result)
            else:
                result += temp_result
        cursor.close()
        return result

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool):
        if hasattr(self, 'connection') and value != self._autocommit:
            self.connection.autocommit = value
        self._autocommit = value

    def connect(self) -> None:
        self.connection = self.lib.connect(user=self.user, password=self.password, dsn='{}:{}/{}'.format(
            self.host, self.port, self.database), encoding=self.charset)
        self.connection.autocommit = self._autocommit

    def execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                many: bool = False, commit: Optional[bool] = None, cursor: Any = None) -> Union[int, tuple, list]:
        # cx_Oracle.Cursor.execute不能传入None
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        if not many:
            cursor.execute(query, () if args is None else args)
        else:  # executemany: 一句插入多条记录，当语句超出1024000字符时拆分成多个语句；传单条记录需用列表包起来
            cursor.executemany(query, args)
        if commit and not self._autocommit:
            self.commit()
        result = cursor.fetchall() if fetchall else len(args) if many and hasattr(args, '__len__') else 1
        if ori_cursor is None:
            cursor.close()
        return result

    def format(self, query: str, args: Any, raise_error: Optional[bool] = None) -> str:
        # cx_Oracle.Connection没有literal和escape，暂不借鉴mysql实现
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
        if fetchall and (self.dictionary if dictionary is None else dictionary):
            cursor = self.connection.cursor()
            cursor.rowfactory = lambda *args: dict(zip((col[0] for col in cursor.description), args))
            return cursor
        return self.connection.cursor()

    def call_proc(self, name: str, args: Iterable = (), fetchall: bool = True, dictionary: Optional[bool] = None,
                  commit: Optional[bool] = None, empty_string_to_none: Optional[bool] = None,
                  try_times_connect: Union[int, float, None] = None, time_sleep_connect: Union[int, float, None] = None,
                  raise_error: Optional[bool] = None, kwargs: Optional[dict] = None) -> Union[int, tuple, list]:
        # cx_Oracle.Cursor.callproc支持kwargs
        # 执行存储过程
        # name: 存储过程名
        # args: 存储过程参数(不能为None，要可迭代)
        # fetchall=False: return成功执行数(1)
        if kwargs is None:
            kwargs = {}
        if try_times_connect is None:
            try_times_connect = self.try_times_connect
        if time_sleep_connect is None:
            time_sleep_connect = self.time_sleep_connect
        if raise_error is None:
            raise_error = self.raise_error
        if args and (self.empty_string_to_none if empty_string_to_none is None else empty_string_to_none):
            args = tuple(each if each != '' else None for each in args)
        cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        try_count_connect = 0
        while True:
            try:
                cursor.callproc(name, args, kwargs)
                if commit and not self._autocommit:
                    self.commit()
                result = cursor.fetchall() if fetchall else 1
                cursor.close()
                return result
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  proc: {}  args: {}'.format(
                            str(type(e))[8:-2], try_count_connect, e, name, args), exc_info=True)
                    if raise_error:
                        cursor.close()
                        raise e
                    break
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  proc: {}  args: {}'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e, name, args), exc_info=True)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                self.rollback()
                if self.log:
                    self.logger.error('{}: {}  proc: {}  args: {}'.format(str(type(e))[8:-2], e, name, args),
                                      exc_info=True)
                if raise_error:
                    cursor.close()
                    raise e
                break
        if fetchall:
            return ()
        return 0

    def call_func(self, name: str, return_type: type, args: Iterable = (), fetchall: bool = True,
                  dictionary: Optional[bool] = None, commit: Optional[bool] = None,
                  empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                  time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                  kwargs: Optional[dict] = None) -> Union[int, tuple, list]:
        # 执行函数
        # name: 函数名
        # return_type: 返回值的类型(必填)，参见：
        #              https://cx-oracle.readthedocs.io/en/latest/user_guide/plsql_execution.html#plsqlfunc
        #              https://cx-oracle.readthedocs.io/en/latest/api_manual/cursor.html#Cursor.callfunc
        # args: 函数参数(不能为None，要可迭代)
        # fetchall=False: return成功执行数(1)
        if kwargs is None:
            kwargs = {}
        if try_times_connect is None:
            try_times_connect = self.try_times_connect
        if time_sleep_connect is None:
            time_sleep_connect = self.time_sleep_connect
        if raise_error is None:
            raise_error = self.raise_error
        if args and (self.empty_string_to_none if empty_string_to_none is None else empty_string_to_none):
            args = tuple(each if each != '' else None for each in args)
        cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        try_count_connect = 0
        while True:
            try:
                cursor.callfunc(name, return_type, args, kwargs)
                if commit and not self._autocommit:
                    self.commit()
                result = cursor.fetchall() if fetchall else 1
                cursor.close()
                return result
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  func: {}  args: {}'.format(
                            str(type(e))[8:-2], try_count_connect, e, name, args), exc_info=True)
                    if raise_error:
                        cursor.close()
                        raise e
                    break
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  func: {}  args: {}'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e, name, args), exc_info=True)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                self.rollback()
                if self.log:
                    self.logger.error('{}: {}  func: {}  args: {}'.format(str(type(e))[8:-2], e, name, args),
                                      exc_info=True)
                if raise_error:
                    cursor.close()
                    raise e
                break
        if fetchall:
            return ()
        return 0
