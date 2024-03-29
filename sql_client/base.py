# -*- coding: utf-8 -*-

import os
import time
import contextlib
import enum
import re
import itertools
import functools
from typing import Any, Union, Optional, Tuple, Iterable, Collection, Callable, Sequence, Generator


class Notset:
    pass


NOTSET = Notset()


class Paramstyle(enum.IntEnum):
    pyformat = 0
    format = 1
    named = 2
    numeric = 3
    qmark = 4


class SqlClient(object):
    lib = None
    _pattern = {Paramstyle.pyformat: re.compile(r'(?<![%\\])%\(([\w$]+)\)s'),
                Paramstyle.format: re.compile(r'(?<![%\\])%s'),
                Paramstyle.named: re.compile(r'(?<![:\w$\\]):([a-zA-Z_$][\w$]*)(?!:)'),
                Paramstyle.numeric: re.compile(r'(?<![:\d\\]):(\d+)(?!:)'),
                Paramstyle.qmark: re.compile(r'(?<!\\)\?')}
    _pattern_esc = {Paramstyle.pyformat: re.compile(r'[%\\](%\([\w$]+\)s)'),
                    Paramstyle.format: re.compile(r'[%\\](%s)'),
                    Paramstyle.named: re.compile(r'\\(:[a-zA-Z_$][\w$]*)(?!:)'),
                    Paramstyle.numeric: re.compile(r'\\(:\d+)(?!:)'),
                    Paramstyle.qmark: re.compile(r'\\(\?)')}
    _repl = {Paramstyle.pyformat: r'%(\1)s',
             Paramstyle.format: '%s',
             Paramstyle.named: r':\1',
             Paramstyle.numeric: None,
             Paramstyle.qmark: '?'}
    _repl_format = {Paramstyle.pyformat: r'%({})s',
                    Paramstyle.format: None,
                    Paramstyle.named: r':{}',
                    Paramstyle.numeric: r':{}',
                    Paramstyle.qmark: None}

    # lib模块的以下属性被下列方法使用：
    # lib.ProgrammingError: close
    # lib.InterfaceError, lib.OperationalError: ping, try_connect, try_execute, call_proc
    # lib.cursors.DictCursor: query, call_proc
    # lib.connect: connect

    def __init__(self, host: Optional[str] = None, port: Union[int, str, None] = None, user: Optional[str] = None,
                 password: Optional[str] = None, database: Optional[str] = None, charset: Optional[str] = None,
                 autocommit: bool = True, connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: str = 'INSERT INTO', dictionary: bool = False, escape_auto_format: bool = False,
                 escape_formatter: str = '{}', empty_string_to_none: bool = True, args_to_dict: Optional[bool] = None,
                 to_paramstyle: Optional[Paramstyle] = Paramstyle.format, try_reconnect: bool = True,
                 try_times_connect: Union[int, float] = 3, time_sleep_connect: Union[int, float] = 3,
                 raise_error: bool = False, exc_info: Optional[bool] = None):
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
        if port is None and host is not None and ':' in host:
            host, port = host.rsplit(':', 1)
        self.host = host
        self.port = int(port) if isinstance(port, str) else port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self._autocommit = autocommit
        self.temp_autocommit = None
        self.log = log
        if log:
            import logging
            self.logger = logging.getLogger(__name__)
        self.table = table
        self.statement_save_data = statement_save_data
        self.dictionary = dictionary
        self.escape_auto_format = escape_auto_format
        self.escape_formatter = escape_formatter  # sqlserver使用[], 不能只记一个字符
        self.empty_string_to_none = empty_string_to_none
        self.args_to_dict = args_to_dict
        self.to_paramstyle = to_paramstyle
        self.try_reconnect = try_reconnect
        self.try_times_connect = try_times_connect
        self.time_sleep_connect = time_sleep_connect
        self.raise_error = raise_error
        self.exc_info = exc_info
        self.connected = False
        self.connection = None
        if connect_now:
            self.try_connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
              chunksize: Optional[int] = None, not_one_by_one: bool = True, auto_format: bool = False,
              keys: Union[str, Collection[str], None] = None, commit: Optional[bool] = None,
              escape_auto_format: Optional[bool] = None, escape_formatter: Optional[str] = None,
              empty_string_to_none: Optional[bool] = None, args_to_dict: Union[bool, Notset, None] = NOTSET,
              to_paramstyle: Union[Paramstyle, Notset, None] = NOTSET, keep_cursor: Optional[bool] = False,
              cursor: Any = None, try_times_connect: Union[int, float, None] = None,
              time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
              exc_info: Union[bool, Notset, None] = NOTSET, call: Optional[Callable] = None
              ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                         Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator], Any]]:
        # args 支持单条记录: list/tuple/dict, 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 注意此时query会被format一次; args_to_dict视为False;
        #                   首条记录需为dict(not_one_by_one=False时所有记录均需为dict), 或者含除自增字段外所有字段并按顺序排好各字段值, 或者自行传入keys
        # fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        # args_to_dict=None: 不做dict和list之间转换; args_to_dict=False: dict强制转为list; args_to_dict=NOTSET: 读取默认配置
        # keep_cursor: 返回(result, cursor), 并且不自动关闭cursor;
        #              如果args为多条记录且not_one_by_one=False且设置了chunksize且fetchall=True(仅此情况会使用多个cursor), 则只会保留最后一个cursor
        if cursor is not None:
            self.set_connection()
        if call is None:
            call = functools.partial(self.try_execute, call=None)
        if args and not hasattr(args, '__getitem__') and hasattr(args, '__iter__'):  # set, Generator, range
            args = tuple(args)
        if args is None or hasattr(args, '__len__') and not isinstance(args, str) and not args:
            return call(query, args, fetchall, dictionary, chunksize, False, commit, keep_cursor, cursor,
                        try_times_connect, time_sleep_connect, raise_error, exc_info)
        if escape_auto_format is None:
            escape_auto_format = self.escape_auto_format
        if auto_format:
            args_to_dict = False
        if to_paramstyle is NOTSET:
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
        args, keys, is_multiple, is_key_generated = self.standardize_args(args, None, empty_string_to_none,
                                                                          args_to_dict, True, keys, nums)
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
            return call(query, args, fetchall, dictionary, chunksize, is_multiple, commit, keep_cursor, cursor,
                        try_times_connect, time_sleep_connect, raise_error, exc_info)
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
        cursor = self._before_query_and_get_cursor(fetchall, dictionary) if chunksize is None or not fetchall else None
        for arg in args:
            if auto_format and keys is None:
                query = ori_query.format('({})'.format(','.join(map(escape_formatter.format if escape_auto_format else
                                                                    str, arg)))
                                         if isinstance(arg, dict) and not is_key_generated else '',
                                         ','.join(self.paramstyle_formatter(arg, to_paramstyle)))
            temp_result = call(query, arg, fetchall, dictionary, chunksize, not_one_by_one, commit, keep_cursor, cursor,
                               try_times_connect, time_sleep_connect, raise_error, exc_info)
            if keep_cursor:
                if (chunksize is None or not fetchall) and cursor is not None:
                    cursor.close()
                temp_result, cursor = temp_result
            if fetchall:
                result.append(temp_result)
            else:
                result += temp_result
        if keep_cursor:
            return result, cursor
        if cursor is not None:
            cursor.close()
        return result

    def save_data(self, args: Any, table: Optional[str] = None, statement: Optional[str] = None,
                  extra: Optional[str] = None, not_one_by_one: Optional[bool] = False,
                  keys: Union[str, Collection[str], None] = None, commit: Optional[bool] = None,
                  escape_auto_format: Optional[bool] = None, escape_formatter: Optional[str] = None,
                  empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                  time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                  exc_info: Union[bool, Notset, None] = NOTSET) -> Union[int, tuple, list]:
        # data_list 支持单条记录: list/tuple/dict, 或多条记录: list/tuple/set[list/tuple/dict]
        # 首条记录需为dict(one_by_one=True时所有记录均需为dict), 或者含除自增字段外所有字段并按顺序排好各字段值, 或者自行传入keys
        # 默认not_one_by_one=False: 为了部分记录无法插入时能够单独跳过这些记录(有log)
        # fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        if args is None or hasattr(args, '__len__') and not isinstance(args, str) and not args:
            return 0
        query = '{} {}{{}} VALUES({{}}){}'.format(
            self.statement_save_data if statement is None else statement, self.table if table is None else table,
            ' {}'.format(extra) if extra is not None else '')
        return self.query(query, args, False, False, None, not_one_by_one, True, keys, commit, escape_auto_format,
                          escape_formatter, empty_string_to_none, False, NOTSET, False, None, try_times_connect,
                          time_sleep_connect, raise_error, exc_info, None)

    def select_to_try(self, table: Optional[str] = None, num: Union[int, str, None] = 1,
                      key_fields: Union[str, Iterable[str]] = 'id', extra_fields: Union[str, Iterable[str], None] = '',
                      tried_field: Optional[str] = None, tried: Union[int, str, Notset, None] = 'between',
                      tried_min: Union[int, str, None] = 1, tried_max: Union[int, str, None] = 5,
                      tried_after: Union[int, str, Notset, None] = '-', finished_field: Optional[str] = None,
                      finished: Union[int, str, None] = 0, next_time_field: Optional[str] = None,
                      next_time: Union[int, float, str, Notset, None] = None,
                      next_time_after: Union[int, float, str, Notset, None] = NOTSET, lock: bool = True,
                      dictionary: Optional[bool] = None, autocommit_after: Optional[bool] = None,
                      select_where: Optional[str] = None, select_extra: str = '', set_extra: Optional[str] = '',
                      update_set: Optional[str] = None, update_where: Optional[str] = None, update_extra: str = '',
                      empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                      time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                      exc_info: Union[bool, Notset, None] = NOTSET, call: Optional[Callable] = None
                      ) -> Union[int, tuple, list]:
        # key_fields: update一句where部分使用
        # extra_fields: 不在update一句使用, return结果包含key_fields和extra_fields
        # tried_field, finished_field, next_time_field字段传入与否分别决定相关逻辑启用与否, 默认值None表示不启用
        # tried: 默认值'between'表示取tried_min<=tried_field<=tried_max, 也可传入'>=0'等(传入NOTSET表示不限制),
        #        如需多个条件, 可传入NOTSET并往select_extra传入例如' and (<tried_field> is null or <tried_field> <= <time>)'
        # tried_after: 默认值'-'表示取tried_field当前值的相反数, 也可传入'+1'等
        #              (传入NOTSET表示不修改, 传入None则设为null, 传入''则根据empty_string_to_none决定是否转为null)
        # next_time: 默认值None表示取next_time_field<=当前timestamp整数部分(注意取不到为null的记录), 传入NOTSET表示不限制, 如需多个条件,
        #            可传入NOTSET并往select_extra传入例如' and (<next_time_field> is null or <next_time_field> <= <time>)'
        # next_time_after: 默认值NOTSET表示不修改, 传入None则设为null, 传入''则根据empty_string_to_none决定是否转为null
        # select_where: 不为None则替换select一句的where部分(为''时删除where)
        # update_set: 不为None则替换update一句的set部分
        # update_where: 不为None则替换update一句的where部分
        if table is None:
            table = self.table
        if isinstance(key_fields, str):
            key_fields_list = [key.strip() for key in key_fields.split(',')]
        else:
            key_fields_list = key_fields
            key_fields = ','.join(key_fields)
        if extra_fields is not None and not isinstance(extra_fields, str):
            extra_fields = ','.join(extra_fields)
        args = []
        if select_where is None:
            if not tried_field or tried is NOTSET:
                select_tried = ''
            elif tried is None or tried == 'null':
                select_tried = tried_field + ' is null'
            elif tried == 'between':
                select_tried = '{} between {} and {}'.format(tried_field, tried_min, tried_max)
            elif isinstance(tried, str) and tried.lstrip().startswith(('>', '=', '<', 'between')):
                if tried.startswith('between'):
                    select_tried = tried_field + ' ' + tried
                else:
                    select_tried = tried_field + tried
            elif isinstance(tried, int):
                select_tried = '{}={}'.format(tried_field, tried)
            else:
                select_tried = tried_field + '=%s'
                args.append(tried)
            if not finished_field:
                select_finished = ''
            elif finished is None or finished == 'null':
                select_finished = finished_field + ' is null'
            elif isinstance(finished, str) and finished.lstrip().startswith(('>', '=', '<', 'between')):
                if finished.startswith('between'):
                    select_finished = finished_field + ' ' + finished
                else:
                    select_finished = finished_field + finished
            elif isinstance(finished, int):
                select_finished = '{}={}'.format(finished_field, finished)
            else:
                select_finished = finished_field + '=%s'
                args.append(finished)
            if not next_time_field or next_time is NOTSET:
                select_next_time = ''
            elif next_time is None:
                select_next_time = '{}<={}'.format(next_time_field, int(time.time()))
            elif next_time == 'null':
                select_next_time = next_time_field + ' is null'
            elif isinstance(next_time, str) and next_time.lstrip().startswith(('>', '=', '<', 'between')):
                if next_time.startswith('between'):
                    select_next_time = next_time_field + ' ' + next_time
                else:
                    select_next_time = next_time_field + next_time
            elif isinstance(next_time, (int, float)):
                select_next_time = '{}<={}'.format(next_time_field,
                                                   int(time.time()) + next_time if next_time < 10 ** 9 else next_time)
            else:
                select_next_time = next_time_field + '<=%s'
                args.append(next_time)
            select_where = ' where ' + ' and '.join(filter(None, (select_tried, select_finished, select_next_time)))
        elif select_where:
            if select_where.startswith('where'):
                select_where = ' ' + select_where
            elif not select_where.startswith(' where'):
                select_where = ' where ' + select_where.lstrip(' ')
        query = 'select {}{} from {}{}{}{}{}'.format(key_fields, ',' + extra_fields if extra_fields else '', table,
                                                     select_where, select_extra, ' limit {}'.format(num) if num else '',
                                                     ' for update' if lock else '')
        transaction = self.begin()
        result = self.query(query, args, fetchall=True, dictionary=dictionary, commit=False,
                            empty_string_to_none=empty_string_to_none, try_times_connect=try_times_connect,
                            time_sleep_connect=time_sleep_connect, raise_error=raise_error, exc_info=exc_info,
                            call=call)
        if not result:
            self.commit(transaction)
            if autocommit_after is not None:
                self.autocommit = autocommit_after
            return result
        args = []
        if not tried_field or tried_after is NOTSET:
            update_tried = ''
        elif tried_after == '-':
            update_tried = '{0}=-{0}'.format(tried_field)
        elif tried_after == '+1':
            update_tried = '{0}={0}+1'.format(tried_field)
        elif tried_after == '-+1':
            update_tried = '{0}=-{0}+1'.format(tried_field)
        elif isinstance(tried_after, str) and tried_after.lstrip().startswith('='):
            update_tried = tried_field + tried_after
        elif isinstance(tried_after, int):
            update_tried = '{}={}'.format(tried_field, tried_after)
        else:
            update_tried = tried_field + '=%s'
            args.append(tried_after)
        if not next_time_field or next_time_after is NOTSET:
            update_next_time = ''
        elif isinstance(next_time_after, (int, float)):
            update_next_time = '{}={}'.format(next_time_field,
                                              int(time.time()) + next_time_after if next_time_after < 10 ** 9 else
                                              next_time_after)
        elif isinstance(next_time_after, str) and next_time_after.lstrip().startswith('='):
            update_next_time = next_time_field + next_time_after
        else:
            update_next_time = next_time_field + '=%s'
            args.append(next_time_after)
        if update_where is None:
            update_where = ' or '.join((' and '.join(map('{}=%s'.format, key_fields_list)),) * len(result))
            if dictionary:
                args.extend(row[key] for row in result for key in key_fields_list)
            else:
                args.extend(row[i] for row in result for i in range(len(key_fields_list)))
        elif update_where.startswith('where'):
            update_where = update_where[5:].lstrip(' ')
        elif update_where.startswith(' where'):
            update_where = update_where[6:].lstrip(' ')
        query = 'update {} set {} where {}{}'.format(table, ','.join(filter(None, (update_tried, update_next_time)))
                                                            + set_extra if update_set is None else update_set,
                                                     update_where, update_extra)
        is_success = self.query(query, args, fetchall=False, commit=False, empty_string_to_none=empty_string_to_none,
                                try_times_connect=try_times_connect, time_sleep_connect=time_sleep_connect,
                                raise_error=raise_error, exc_info=exc_info, call=call)
        if is_success:
            self.commit(transaction)
        else:
            result = ()
            self.rollback(transaction)
        if autocommit_after is not None:
            self.autocommit = autocommit_after
        return result

    def end_try(self, result: Optional[Iterable], table: Optional[str] = None,
                key_fields: Union[str, Iterable[str], None] = None, tried_field: Optional[str] = None,
                tried: Union[int, str, None] = 0, finished_field: Optional[str] = None,
                finished: Union[int, str, None] = 1, next_time_field: Optional[str] = None,
                next_time: Union[int, float, str, None] = '=0', commit: bool = True, set_extra: Optional[str] = '',
                update_set: Optional[str] = None, update_where: Optional[str] = None, update_extra: str = '',
                empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                exc_info: Union[bool, Notset, None] = NOTSET, call: Optional[Callable] = None) -> int:
        # key_fields为''或None时, result需为dict或list[dict], key_fields取result的keys
        # tried_field, finished_field, next_time_field字段传入与否分别决定相关逻辑启用与否, 默认值None表示不启用
        # update_where: 不为None则替换update一句的where部分
        result, _ = self.standardize_args(result, True, False, None, False)
        if not result:
            return 0
        if table is None:
            table = self.table
        if not key_fields:
            key_fields = tuple(result[0].keys())
        elif isinstance(key_fields, str):
            key_fields = [key.strip() for key in key_fields.split(',')]
        args = []
        if not tried_field:
            update_tried = ''
        elif tried == '-+1':
            update_tried = '{0}=-{0}+1'.format(tried_field)
        elif tried == '-':
            update_tried = '{0}=-{0}'.format(tried_field)
        elif tried == '+1':
            update_tried = '{0}={0}+1'.format(tried_field)
        elif isinstance(tried, str) and tried.lstrip().startswith('='):
            update_tried = tried_field + tried
        elif isinstance(tried, int):
            update_tried = '{}={}'.format(tried_field, tried)
        else:
            update_tried = tried_field + '=%s'
            args.append(tried)
        if not finished_field:
            update_finished = ''
        elif isinstance(finished, str) and finished.lstrip().startswith('='):
            update_finished = finished_field + finished
        elif isinstance(finished, int):
            update_finished = '{}={}'.format(finished_field, finished)
        else:
            update_finished = finished_field + '=%s'
            args.append(finished)
        if not next_time_field:
            update_next_time = ''
        elif isinstance(next_time, (int, float)):
            update_next_time = '{}={}'.format(next_time_field,
                                              int(time.time()) + next_time if next_time < 10 ** 9 else next_time)
        elif isinstance(next_time, str) and next_time.lstrip().startswith('='):
            update_next_time = next_time_field + next_time
        else:
            update_next_time = next_time_field + '=%s'
            args.append(next_time)
        if update_where is None:
            update_where = ' or '.join((' and '.join(map('{}=%s'.format, key_fields)),) * len(result))
            if isinstance(result[0], dict):
                args.extend(row[key] for row in result for key in key_fields)
            else:
                args.extend(row[i] for row in result for i in range(len(key_fields)))
        elif update_where.startswith('where'):
            update_where = update_where[5:].lstrip(' ')
        elif update_where.startswith(' where'):
            update_where = update_where[6:].lstrip(' ')
        query = 'update {} set {} where {}{}'.format(table, ','.join(filter(None, (
            update_tried, update_finished, update_next_time))) + set_extra if update_set is None else update_set,
                                                     update_where, update_extra)
        return self.query(query, args, fetchall=False, commit=commit, empty_string_to_none=empty_string_to_none,
                          try_times_connect=try_times_connect, time_sleep_connect=time_sleep_connect,
                          raise_error=raise_error, exc_info=exc_info, call=call)

    def fail_try(self, result: Optional[Iterable], table: Optional[str] = None,
                 key_fields: Union[str, Iterable[str], None] = None, tried_field: Optional[str] = None,
                 tried: Union[int, str, None] = '-+1', finished_field: Optional[str] = None,
                 finished: Union[int, str, None] = 0, next_time_field: Optional[str] = None,
                 next_time: Union[int, float, str, None] = 300, commit: bool = True, set_extra: Optional[str] = '',
                 update_set: Optional[str] = None, update_where: Optional[str] = None, update_extra: str = '',
                 empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                 time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                 exc_info: Union[bool, Notset, None] = NOTSET, call: Optional[Callable] = None) -> int:
        # 复用end_try, 仅改变tried, finished, next_time参数默认值
        return self.end_try(result, table, key_fields, tried_field, tried, finished_field, finished, next_time_field,
                            next_time, commit, set_extra, update_set, update_where, update_extra, empty_string_to_none,
                            try_times_connect, time_sleep_connect, raise_error, exc_info, call)

    def cancel_try(self, result: Optional[Iterable], table: Optional[str] = None,
                   key_fields: Union[str, Iterable[str], None] = None, tried_field: Optional[str] = None,
                   tried: Union[int, str, None] = '-', finished_field: Optional[str] = None,
                   finished: Union[int, str, None] = 0, next_time_field: Optional[str] = None,
                   next_time: Union[int, float, str, None] = '=0', commit: bool = True, set_extra: Optional[str] = '',
                   update_set: Optional[str] = None, update_where: Optional[str] = None, update_extra: str = '',
                   empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                   time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                   exc_info: Union[bool, Notset, None] = NOTSET, call: Optional[Callable] = None) -> int:
        # 取消尝试, 恢复select_to_try以前的原状
        # 复用end_try, 仅改变tried参数默认值
        # finished_field, next_time_field建议不填写, 以保持原状
        return self.end_try(result, table, key_fields, tried_field, tried, finished_field, finished, next_time_field,
                            next_time, commit, set_extra, update_set, update_where, update_extra, empty_string_to_none,
                            try_times_connect, time_sleep_connect, raise_error, exc_info, call)

    def close(self, try_close: bool = True) -> None:
        self.connected = False
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
        if self.connection is not None and value != self._autocommit:
            self.connection.autocommit(value)
        self._autocommit = value

    @contextlib.contextmanager
    def transaction(self):
        # yield: None or transaction
        transaction = self.begin()
        try:
            yield transaction
            self.commit(transaction)
        except Exception as e:
            self.rollback(transaction)
            raise e

    def begin(self) -> None:
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()
        self.connection.begin()

    def commit(self, transaction=None) -> None:
        self.connection.commit()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def rollback(self, transaction=None) -> None:
        if self.connection is not None:
            self.connection.rollback()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def connect(self) -> None:
        self.connection = self.lib.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.database, charset=self.charset, autocommit=self._autocommit)
        self.connected = True

    def reconnect(self, exc_info: Union[bool, Notset, None] = NOTSET) -> None:
        self.connect()

    def set_connection(self) -> None:
        if not self.connected or self.connection is None:
            self.try_connect()

    def try_connect(self, try_reconnect: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                    time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                    exc_info: Union[bool, Notset, None] = NOTSET) -> None:
        if try_reconnect is None:
            try_reconnect = self.try_reconnect
        if try_times_connect is None:
            try_times_connect = self.try_times_connect
        if time_sleep_connect is None:
            time_sleep_connect = self.time_sleep_connect
        if raise_error is None:
            raise_error = self.raise_error
        if exc_info is NOTSET:
            exc_info = self.exc_info
        try_count_connect = 0
        while True:
            try:
                if try_reconnect:
                    self.reconnect()
                else:
                    self.connect()
                return
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  (in try_connect)'.format(
                            str(type(e))[8:-2], try_count_connect, e),
                            exc_info=not raise_error if exc_info is None else exc_info)
                    if raise_error:
                        raise e
                    return
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  (in try_connect)'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e),
                        exc_info=True if exc_info is None else exc_info)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                if self.log:
                    self.logger.error('{}: {}  (in try_connect)'.format(str(type(e))[8:-2], e),
                                      exc_info=not raise_error if exc_info is None else exc_info)
                if raise_error:
                    raise e
                return

    def try_execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                    chunksize: Optional[int] = None, many: bool = False, commit: Optional[bool] = None,
                    keep_cursor: Optional[bool] = False, cursor: Any = None,
                    try_times_connect: Union[int, float, None] = None,
                    time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                    exc_info: Union[bool, Notset, None] = NOTSET, call: Optional[Callable] = None
                    ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                               Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator], Any]]:
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        if try_times_connect is None:
            try_times_connect = self.try_times_connect
        if time_sleep_connect is None:
            time_sleep_connect = self.time_sleep_connect
        if raise_error is None:
            raise_error = self.raise_error
        if exc_info is NOTSET:
            exc_info = self.exc_info
        if call is None:
            call = self.execute
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        try_count_connect = 0
        while True:
            try:
                result = call(query, args, fetchall, dictionary, chunksize, many, commit, keep_cursor, cursor)
                if ori_cursor is None and (
                        chunksize is None or not fetchall) and cursor is not None and not keep_cursor:
                    cursor.close()
                return result
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  {}'.format(
                            str(type(e))[8:-2], try_count_connect, e, self._query_log_text(query, args, cursor)),
                            exc_info=not raise_error if exc_info is None else exc_info)
                    if raise_error:
                        if ori_cursor is None and cursor is not None:
                            cursor.close()
                        raise e
                    break
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  {}'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e,
                        self._query_log_text(query, args, cursor)), exc_info=True if exc_info is None else exc_info)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                self.rollback()
                if self.log:
                    self.logger.error('{}: {}  {}'.format(
                        str(type(e))[8:-2], e, self._query_log_text(query, args, cursor)),
                        exc_info=not raise_error if exc_info is None else exc_info)
                if raise_error:
                    if ori_cursor is None and cursor is not None:
                        cursor.close()
                    raise e
                break
        if fetchall:
            return ()
        return 0

    def execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                chunksize: Optional[int] = None, many: bool = False, commit: Optional[bool] = None,
                keep_cursor: Optional[bool] = False, cursor: Any = None
                ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                           Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator], Any]]:
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        if not many:
            cursor.execute(query, args)
        else:  # executemany: 一句插入多条记录, 当语句超出1024000字符时拆分成多个语句; 传单条记录需用列表包起来
            cursor.executemany(query, args)
        if commit and not self._autocommit:
            self.commit()
        result = (cursor.fetchall() if chunksize is None else self._fetchmany_generator(cursor, chunksize, keep_cursor)
                  ) if fetchall else len(args) if many and hasattr(args, '__len__') else 1
        if keep_cursor:
            return result, cursor
        if ori_cursor is None and (chunksize is None or not fetchall):
            cursor.close()
        return result

    @staticmethod
    def _fetchmany_generator(cursor, chunksize, keep_cursor):
        while True:
            result = cursor.fetchmany(chunksize)
            if not result:
                if not keep_cursor:
                    cursor.close()
                return
            yield result

    def ping(self) -> None:
        self.set_connection()
        try:
            self.connection.ping()
        except (self.lib.InterfaceError, self.lib.OperationalError):
            # MySQLdb._exceptions.OperationalError: (2013, 'Lost connection to MySQL server during query')
            # MySQLdb._exceptions.OperationalError: (2006, 'MySQL server has gone away')
            # _mysql_exceptions.InterfaceError: (0, '')
            self.close()
            self.try_connect()
        except AttributeError:
            # AttributeError: 'SqlClient' object has no attribute 'connection'
            # AttributeError: 'NoneType' object has no attribute 'ping'
            self.try_connect()

    def standardize_args(self, args: Any, to_multiple: Optional[bool] = None,
                         empty_string_to_none: Optional[bool] = None, args_to_dict: Union[bool, Notset, None] = NOTSET,
                         get_info: bool = False, keys: Optional[Iterable[str]] = None,
                         nums: Optional[Iterable[int]] = None
                         ) -> Union[Tuple[Union[Sequence, dict, None], Optional[Iterable[str]]],
                                    Tuple[Union[Sequence, dict, None], Optional[Iterable[str]], bool, bool]]:
        # get_info=True: 返回值除了args, keys以外还有: 是否multiple, 字典key是否为生成的
        # args_to_dict=None: 不做dict和list之间转换; args_to_dict=False: dict强制转为list; args_to_dict=NOTSET: 读取默认配置
        # 仅在args_to_dict=False且args为dict形式且keys为None时, keys会被修改
        # nums: from_paramstyle=Paramstyle.numeric且to_paramstyle为Paramstyle.format或Paramstyle.qmark且通配符乱序时, 传入通配符数字列表
        if args is None:
            return (None, keys) if not get_info else (None, keys, False, False)
        if not hasattr(args, '__getitem__'):
            if hasattr(args, '__iter__'):  # set, Generator, range
                args = tuple(args)
            else:  # int, etc.
                args = (args,)
        elif isinstance(args, str):
            args = (args,)
        # else: dict, list, tuple, dataset/row, recordcollection/record
        if not args:
            return (args, keys) if not get_info else (args, keys, False, False)
        if to_multiple is None:  # 检测是否multiple
            to_multiple = not isinstance(args, dict) and not isinstance(args[0], str) and (
                    hasattr(args[0], '__getitem__') or hasattr(args[0], '__iter__'))
        if args_to_dict is NOTSET:
            args_to_dict = self.args_to_dict
        if empty_string_to_none is None:
            empty_string_to_none = self.empty_string_to_none
        is_key_generated = False
        if not to_multiple:
            if isinstance(args, dict):
                if args_to_dict is False:
                    if keys is None:
                        keys = tuple(args)
                    args = tuple(args[key] if args[key] != '' else None for key in keys
                                 ) if empty_string_to_none else tuple(map(args.__getitem__, keys))
                elif empty_string_to_none:
                    args = {key: value if value != '' else None for key, value in args.items()}
            else:
                if args_to_dict:
                    if keys is None:
                        to_dict_keys = tuple(map(str, range(1, len(args) + 1)))
                        is_key_generated = True
                    else:
                        to_dict_keys = keys
                    args = {key: value if value != '' else None for key, value in zip(to_dict_keys, args)
                            } if empty_string_to_none else dict(zip(to_dict_keys, args))
                elif nums is not None:
                    args = tuple(args[num] if args[num] != '' else None for num in nums
                                 ) if empty_string_to_none else tuple(map(args.__getitem__, nums))
                elif empty_string_to_none:
                    args = tuple(each if each != '' else None for each in args)
        else:
            if isinstance(args, dict):
                if args_to_dict is False:
                    if keys is None:
                        keys = tuple(args)
                    args = (tuple(args[key] if args[key] != '' else None for key in keys),
                            ) if empty_string_to_none else (tuple(map(args.__getitem__, keys)),)
                else:
                    args = ({key: value if value != '' else None for key, value in args.items()},
                            ) if empty_string_to_none else (args,)
            elif isinstance(args[0], dict):
                if args_to_dict is False:
                    if keys is None:
                        keys = tuple(args[0])
                    args = tuple(tuple(each[key] if each[key] != '' else None for key in keys) for each in args
                                 ) if empty_string_to_none else tuple(
                        tuple(map(each.__getitem__, keys)) for each in args)
                elif empty_string_to_none:
                    args = tuple({key: value if value != '' else None for key, value in each.items()} for each in args)
            else:
                if args_to_dict:
                    if keys is None:
                        to_dict_keys = tuple(map(str, range(1, len(args) + 1)))
                        is_key_generated = True
                    else:
                        to_dict_keys = keys
                    if isinstance(args[0], str):
                        args = ({key: value if value != '' else None for key, value in zip(to_dict_keys, args)},
                                ) if empty_string_to_none else (dict(zip(to_dict_keys, args)),)
                    elif not hasattr(args[0], '__getitem__'):
                        if hasattr(args[0], '__iter__'):  # list[set, Generator, range]
                            # mysqlclient, pymysql均只支持dict, list, tuple, 不支持set, Generator等
                            args = tuple({key: value if value != '' else None for key, value in zip(to_dict_keys, each)}
                                         for each in args) if empty_string_to_none else tuple(
                                dict(zip(to_dict_keys, each)) for each in args)
                        else:  # list[int, etc.]
                            args = ({key: value if value != '' else None for key, value in zip(to_dict_keys, args)},
                                    ) if empty_string_to_none else (dict(zip(to_dict_keys, args)),)
                    else:
                        args = tuple({key: value if value != '' else None for key, value in zip(to_dict_keys, each)}
                                     for each in args) if empty_string_to_none else tuple(dict(zip(to_dict_keys, each))
                                                                                          for each in args)
                elif nums is not None:
                    if isinstance(args[0], str):
                        args = (tuple(args[num] if args[num] != '' else None for num in nums),
                                ) if empty_string_to_none else (tuple(map(args.__getitem__, nums)),)
                    elif not hasattr(args[0], '__getitem__'):
                        if hasattr(args[0], '__iter__'):  # list[set, Generator, range]
                            # mysqlclient, pymysql均只支持dict, list, tuple, 不支持set, Generator等
                            args = tuple(tuple(each[num] if each[num] != '' else None for num in nums) for each in
                                         map(tuple, args)) if empty_string_to_none else tuple(
                                tuple(map(each.__getitem__, nums)) for each in map(tuple, args))
                        else:  # list[int, etc.]
                            args = (tuple(args[num] if args[num] != '' else None for num in nums),
                                    ) if empty_string_to_none else (tuple(map(args.__getitem__, nums)),)
                    else:
                        args = tuple(tuple(each[num] if each[num] != '' else None for num in nums) for each in args
                                     ) if empty_string_to_none else tuple(
                            tuple(map(each.__getitem__, nums)) for each in args)
                elif isinstance(args[0], str):
                    args = (tuple(each if each != '' else None for each in args),) if empty_string_to_none else (args,)
                elif not hasattr(args[0], '__getitem__'):
                    if hasattr(args[0], '__iter__'):  # list[set, Generator, range]
                        # mysqlclient, pymysql均只支持dict, list, tuple, 不支持set, Generator等
                        args = tuple(tuple(e if e != '' else None for e in each) for each in args
                                     ) if empty_string_to_none else tuple(tuple(each) for each in args)
                    else:  # list[int, etc.]
                        args = (tuple(each if each != '' else None for each in args),) if empty_string_to_none else (
                            args,)
                elif empty_string_to_none:
                    args = tuple(tuple(e if e != '' else None for e in each) for each in args)
        return (args, keys) if not get_info else (args, keys, to_multiple, is_key_generated)

    @classmethod
    def judge_paramstyle(cls, query: str, first: Optional[Paramstyle] = None) -> Optional[Paramstyle]:
        if first is not None and cls._pattern[first].search(query):
            return first
        for from_paramstyle, from_pattern in cls._pattern.items():
            if from_paramstyle != first and from_pattern.search(query):
                return from_paramstyle
        else:
            return None

    @classmethod
    def transform_paramstyle(cls, query: str, to_paramstyle: Paramstyle,
                             from_paramstyle: Union[Paramstyle, Notset, None] = NOTSET) -> str:
        # args需预先转换为dict或list形式（与to_paramstyle相对应的那一种）
        # from_paramstyle=None: 无paramstyle; from_paramstyle=NOTSET: 未传入该参数
        if from_paramstyle is NOTSET:
            from_paramstyle = cls.judge_paramstyle(query, to_paramstyle)
        if from_paramstyle is None:
            return query
        if from_paramstyle != to_paramstyle:
            if to_paramstyle == Paramstyle.numeric or to_paramstyle in (
                    Paramstyle.pyformat, Paramstyle.named) and from_paramstyle in (Paramstyle.format, Paramstyle.qmark):
                pos_count = itertools.count(1)
                query = cls._pattern[from_paramstyle].sub(
                    lambda m: cls._repl_format[to_paramstyle].format(str(next(pos_count))), query)
            else:
                query = cls._pattern[from_paramstyle].sub(cls._repl[to_paramstyle], query)
        return cls._pattern_esc[from_paramstyle].sub(r'\1', query)

    @staticmethod
    def paramstyle_formatter(arg: Union[Sequence, dict], paramstyle: Optional[Paramstyle] = None) -> Iterable[str]:
        if paramstyle is None or paramstyle == Paramstyle.format:
            return ('%s',) * len(arg)
        if paramstyle == Paramstyle.pyformat:
            return map('%({})s'.format, arg)
        if paramstyle == Paramstyle.named:
            return map(':{}'.format, arg)
        if paramstyle == Paramstyle.numeric:
            return map(':{}'.format, range(1, len(arg) + 1))
        if paramstyle == Paramstyle.qmark:
            return ('?',) * len(arg)
        raise ValueError(paramstyle)

    def format(self, query: str, args: Any, raise_error: Optional[bool] = None, cursor: Any = None) -> str:
        try:
            if args is None:
                return query
            if isinstance(args, dict):
                new_args = dict((key, self.connection.literal(item)) for key, item in args.items())
                return query % new_args if '%' in query else query.format(**new_args)
            new_args = tuple(map(self.connection.literal, args))
            return query % new_args if '%' in query else query.format(*new_args)
        except Exception as e:
            if raise_error or raise_error is None and self.raise_error:
                raise e
            return query

    def _before_query_and_get_cursor(self, fetchall: bool = True, dictionary: Optional[bool] = None) -> Any:
        if fetchall and (self.dictionary if dictionary is None else dictionary):
            cursor_class = self.lib.cursors.DictCursor
        else:
            cursor_class = None
        self.set_connection()
        return self.connection.cursor(cursor_class)

    def _query_log_text(self, query: str, args: Any, cursor: Any = None) -> str:
        try:
            return 'formatted_query: {}'.format(self.format(query, args, True, cursor))
        except Exception as e:
            return 'query: {}  args: {}  {}: {}'.format(query, args, str(type(e))[8:-2], e)

    def query_file(self, path: str, encoding: Optional[str] = None, args: Any = None, fetchall: bool = True,
                   dictionary: Optional[bool] = None, chunksize: Optional[int] = None, not_one_by_one: bool = True,
                   auto_format: bool = False, keys: Union[str, Collection[str], None] = None,
                   commit: Optional[bool] = None, escape_auto_format: Optional[bool] = None,
                   escape_formatter: Optional[str] = None, empty_string_to_none: Optional[bool] = None,
                   args_to_dict: Union[bool, Notset, None] = NOTSET,
                   to_paramstyle: Union[Paramstyle, Notset, None] = NOTSET, keep_cursor: Optional[bool] = False,
                   cursor: Any = None, try_times_connect: Union[int, float, None] = None,
                   time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None
                   ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                              Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator], Any]]:
        with open(path, encoding=encoding) as f:
            query = f.read()
        return self.query(query, args, fetchall, dictionary, chunksize, not_one_by_one, auto_format, keys, commit,
                          escape_auto_format, escape_formatter, empty_string_to_none, args_to_dict, to_paramstyle,
                          keep_cursor, cursor, try_times_connect, time_sleep_connect, raise_error)

    def _callproc(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                  chunksize: Optional[int] = None, many: bool = False, commit: Optional[bool] = None,
                  keep_cursor: Optional[bool] = False, cursor: Any = None
                  ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                             Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator], Any]]:
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        cursor.callproc(query, args)
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
                  cursor: Any = None, try_times_connect: Union[int, float, None] = None,
                  time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                  exc_info: Union[bool, Notset, None] = NOTSET, call: Optional[Callable] = None
                  ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator],
                             Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], Generator], Any]]:
        # 执行存储过程
        # name: 存储过程名
        # args: 存储过程参数(不能为None, 要可迭代)
        # fetchall=False: return成功执行数(1)
        # keep_cursor: 返回(result, cursor), 并且不自动关闭cursor
        if call is None:
            call = functools.partial(self.try_execute, call=self._callproc)
        return self.query(name, args, fetchall, dictionary, chunksize, not_one_by_one, auto_format, keys, commit,
                          escape_auto_format, escape_formatter, empty_string_to_none, args_to_dict, to_paramstyle,
                          keep_cursor, cursor, try_times_connect, time_sleep_connect, raise_error, exc_info, call)
