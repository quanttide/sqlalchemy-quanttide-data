# -*- coding: utf-8 -*-

import os
import time
import contextlib
import enum
import re
import itertools
from typing import Any, Union, Optional, Tuple, Iterable, Collection, Sequence


class SqlClient(object):
    lib = None

    # lib模块的以下属性被下列方法使用：
    # lib.ProgrammingError: close
    # lib.InterfaceError, lib.OperationalError: ping, try_connect, try_execute, call_proc
    # lib.cursors.DictCursor: query, call_proc
    # lib.connect: connect

    def __new__(cls, *args, **kwargs):
        cls.validate_sql_client_lib()
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, host: Optional[str] = None, port: Union[int, str, None] = None, user: Optional[str] = None,
                 password: Optional[str] = None, database: Optional[str] = None, charset: Optional[str] = None,
                 autocommit: bool = True, connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: str = 'INSERT INTO', dictionary: bool = False, escape_auto_format: bool = False,
                 escape_formatter: str = '{}', empty_string_to_none: bool = True, args_to_dict: Optional[bool] = None,
                 to_paramstyle: Optional[Paramstyle] = Paramstyle.format, try_times_connect: Union[int, float] = 3,
                 time_sleep_connect: Union[int, float] = 3, raise_error: bool = False):
        if host is None:
            host = os.environ.get('DB_HOST')
        if port is None:
            port = os.environ.get('DB_PORT')
        if user is None:
            user = os.environ.get('DB_USER')
        if password is None:
            password = os.environ.get('DB_PASSWORD')
        if database is None:
            database = os.environ.get('DB_DATABASE')
        if table is None:
            table = os.environ.get('DB_TABLE')
        if port is None and ':' in host:
            host, port = host.rsplit(':', 1)
        self.host = host
        self.port = int(port) if isinstance(port, str) else port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self._autocommit = autocommit
        self.temp_autocommit = None
        self.table = table
        self.statement_save_data = statement_save_data
        self.dictionary = dictionary
        self.escape_auto_format = escape_auto_format
        self.escape_formatter = escape_formatter  # sqlserver使用[]，不能只记一个字符
        self.empty_string_to_none = empty_string_to_none
        self.args_to_dict = args_to_dict
        self.to_paramstyle = to_paramstyle
        self.try_times_connect = try_times_connect
        self.time_sleep_connect = time_sleep_connect
        self.raise_error = raise_error
        self.log = log
        if log:
            import logging
            self.logger = logging.getLogger(__name__)
        if connect_now:
            self.try_connect()

    def query(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
              not_one_by_one: bool = True, auto_format: bool = False, keys: Union[str, Collection[str], None] = None,
              commit: Optional[bool] = None, escape_auto_format: Optional[bool] = None,
              escape_formatter: Optional[str] = None, empty_string_to_none: Optional[bool] = None,
              args_to_dict: Union[bool, None, tuple] = (), to_paramstyle: Union[Paramstyle, None, tuple] = (),
              try_times_connect: Union[int, float, None] = None, time_sleep_connect: Union[int, float, None] = None,
              raise_error: Optional[bool] = None) -> Union[int, tuple, list]:
        # args 支持单条记录: list/tuple/dict, 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 注意此时query会被format一次; args_to_dict视为False;
        #                   首条记录需为dict(not_one_by_one=False时所有记录均需为dict), 或者含除自增字段外所有字段并按顺序排好各字段值, 或者自行传入keys
        # fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        # args_to_dict=None: 不做dict和list之间转换; args_to_dict=False: dict强制转为list; args_to_dict=(): 读取默认配置
        if args and not hasattr(args, '__getitem__') and hasattr(args, '__iter__'):  # set, Generator, range
            args = tuple(args)
        if not args and args not in (0, ''):
            return self.try_execute(query, args, fetchall, dictionary, False, commit, try_times_connect,
                                    time_sleep_connect, raise_error)
        if escape_auto_format is None:
            escape_auto_format = self.escape_auto_format
        if auto_format:
            args_to_dict = False
        if isinstance(to_paramstyle, tuple):
            to_paramstyle = self.to_paramstyle
        if to_paramstyle is not None:
            args_to_dict = to_paramstyle in (Paramstyle.pyformat, Paramstyle.named)
        from_paramstyle = self.judge_paramstyle(query, to_paramstyle)
        if keys is None:
            if args_to_dict is False:
                if from_paramstyle in (Paramstyle.pyformat, Paramstyle.named, Paramstyle.numeric):
                    keys = self._pattern[from_paramstyle].findall(query)
            elif to_paramstyle in (Paramstyle.pyformat, Paramstyle.named) and from_paramstyle == Paramstyle.numeric:
                keys = self._pattern[from_paramstyle].findall(query)
        elif isinstance(keys, str):
            keys = tuple(key.strip() for key in keys.split(','))
        nums = None
        if to_paramstyle in (Paramstyle.format, Paramstyle.qmark) and from_paramstyle == Paramstyle.numeric:
            nums = list(map(int, self._pattern[from_paramstyle].findall(query)))
            if nums == sorted(nums):
                nums = None
        args, is_multiple, is_key_generated = self.standardize_args(args, None, empty_string_to_none, args_to_dict,
                                                                    True, keys, nums)
        if from_paramstyle is None:
            for pattern in self._pattern_esc.values():
                query = pattern.sub(r'\1', query)
        else:
            query = self.transform_paramstyle(query, to_paramstyle, from_paramstyle)
        if not is_multiple or not_one_by_one:  # 执行一次
            if auto_format:
                if escape_formatter is None:
                    escape_formatter = self.escape_formatter
                if keys is None:
                    arg = args[0] if is_multiple else args
                    query = query.format('({})'.format(','.join(map(escape_formatter.format if escape_auto_format else
                                                                    str, arg)))
                                         if isinstance(arg, dict) and not is_key_generated else '',
                                         ','.join(self.paramstyle_formatter(arg, to_paramstyle)))
                else:
                    query = query.format('({})'.format(','.join(map(escape_formatter.format, keys)
                                                                if escape_auto_format else keys)),
                                         ','.join(self.paramstyle_formatter(keys, to_paramstyle)))
            return self.try_execute(query, args, fetchall, dictionary, is_multiple, commit, try_times_connect,
                                    time_sleep_connect, raise_error)
        # 依次执行
        ori_query = query
        result = [] if fetchall else 0
        if auto_format:
            if escape_formatter is None:
                escape_formatter = self.escape_formatter
            if keys is not None:
                query = query.format('({})'.format(','.join(map(escape_formatter.format, keys)
                                                            if escape_auto_format else keys)),
                                     ','.join(self.paramstyle_formatter(keys, to_paramstyle)))
        cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        for arg in args:
            if auto_format and keys is None:
                query = ori_query.format('({})'.format(','.join(map(escape_formatter.format if escape_auto_format else
                                                                    str, arg)))
                                         if isinstance(arg, dict) and not is_key_generated else '',
                                         ','.join(self.paramstyle_formatter(arg, to_paramstyle)))
            temp_result = self.try_execute(query, arg, fetchall, dictionary, not_one_by_one, commit, try_times_connect,
                                           time_sleep_connect, raise_error, cursor)
            if fetchall:
                result.append(temp_result)
            else:
                result += temp_result
        cursor.close()
        return result

    def close(self, try_close: bool = True) -> None:
        if try_close:
            try:
                self.connection.close()
            except self.lib.ProgrammingError:
                # _mysql_exceptions.ProgrammingError: closing a closed connection
                pass
        else:
            self.connection.close()

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool):
        if hasattr(self, 'connection') and value != self._autocommit:
            self.connection.autocommit(value)
        self._autocommit = value

    @contextlib.contextmanager
    def transaction(self):
        # return: self
        self.begin()
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()

    def begin(self) -> None:
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()
        self.connection.begin()

    def commit(self) -> None:
        self.connection.commit()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def _before_query_and_get_cursor(self, fetchall: bool = True, dictionary: Optional[bool] = None) -> Any:
        if fetchall and (self.dictionary if dictionary is None else dictionary):
            cursor_class = self.lib.cursors.DictCursor
        else:
            cursor_class = None
        self.set_connection()
        return self.connection.cursor(cursor_class)

    def _query_log_text(self, query: str, args: Any) -> str:
        try:
            return 'formatted_query: {}'.format(self.format(query, args, True))
        except Exception as e:
            return 'query: {}  args: {}  {}: {}'.format(query, args, str(type(e))[8:-2], e)

    def call_proc(self, name: str, args: Iterable = (), fetchall: bool = True, dictionary: Optional[bool] = None,
                  commit: Optional[bool] = None, empty_string_to_none: Optional[bool] = None,
                  try_times_connect: Union[int, float, None] = None, time_sleep_connect: Union[int, float, None] = None,
                  raise_error: Optional[bool] = None) -> Union[int, tuple, list]:
        # 执行存储过程
        # name: 存储过程名
        # args: 存储过程参数(不能为None，要可迭代)
        # fetchall=False: return成功执行数(1)
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
                cursor.callproc(name, args)
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
