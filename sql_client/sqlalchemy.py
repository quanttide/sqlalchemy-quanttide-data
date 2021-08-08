# -*- coding: utf-8 -*-

import os
import time
import contextlib
import re
from typing import Optional, Union, Iterable, Collection, List, Any

import tablib
from sqlalchemy.pool.impl import NullPool
from sqlalchemy import exc
from sqlalchemy.sql.expression import text
from sqlalchemy.engine import create_engine, RootTransaction, Transaction
from sqlalchemy.inspection import inspect

from .base import SqlClient as BaseSqlClient
from ._records import RecordCollection, Record


class SqlClient(BaseSqlClient):
    lib = exc

    def __init__(self, dialect: Optional[str] = None, driver: Optional[str] = None, host: Optional[str] = None,
                 port: Union[int, str, None] = None, user: Optional[str] = None, password: Optional[str] = None,
                 database: Optional[str] = None, charset: Optional[str] = '', autocommit: bool = True,
                 connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: Optional[str] = None, dictionary: bool = False,
                 escape_auto_format: Optional[bool] = None, escape_formatter: Optional[str] = None,
                 empty_string_to_none: bool = True, keep_args_as_dict: bool = True, transform_formatter: bool = True,
                 try_times_connect: Union[int, float] = 3, time_sleep_connect: Union[int, float] = 3,
                 raise_error: bool = False, origin_result: bool = False, dataset: bool = False, is_pool: bool = False,
                 pool_size: int = 1, engine_kwargs: Optional[dict] = None, **kwargs):
        # dialect也可输入完整url；或者将完整url存于环境变量：DATABASE_URL
        # 完整url格式：dialect[+driver]://user:password@host/dbname[?key=value..]
        # 对user和password影响sqlalchemy解析url的字符进行转义(sqlalchemy解析完url会对user和password解转义) (若从dialect或环境变量传入整个url，需提前转义好)
        # sqlalchemy不会对database进行解转义，故database含?时需移至engine_kwargs['connect_args']['database']
        # sqlalchemy 1.3: database含@时也需移至engine_kwargs['connect_args']['database']
        # 优先级: origin_result > dataset > dictionary
        if engine_kwargs is None:
            engine_kwargs = {}
        if dialect is None:
            dialect = os.environ.get('DIALECT') or os.environ.get('dialect') or os.environ.get(
                'DATABASE_URL') or os.environ.get('database_url')
        if ':' in dialect:  # 完整url模式
            url = dialect
            dialect, tail = url.split('://', 1)
            if not dialect.islower():
                dialect = dialect.lower()
                url = '://'.join((dialect, tail))
            if '+' in dialect:
                dialect, driver = dialect.split('+', 1)
            else:
                driver = None
            if dialect == 'sqlserver':
                dialect = 'mssql'
                url = 'mssql' + url[9:]
        else:  # 非完整url模式
            dialect = dialect.lower()
            if '+' in dialect:
                dialect, driver = dialect.split('+', 1)
            else:
                if driver is None:
                    driver = os.environ.get('DRIVER') or os.environ.get('driver')
                if driver is not None:
                    driver = driver.lower()
            if dialect == 'sqlserver':
                dialect = 'mssql'
            if host is None:
                host = os.environ.get('HOST') or os.environ.get('host')
            if port is None:
                port = os.environ.get('PORT') or os.environ.get('port')
            if user is None:
                user = os.environ.get('USER') or os.environ.get('user')
            if user is not None:
                user = user.replace('%', '%25').replace(':', '%3A').replace('/', '%2F')
            if password is None:
                password = os.environ.get('PASSWORD') or os.environ.get('password')
            if password is not None:
                password = password.replace('%', '%25').replace('@', '%40')
            if database is None:
                database = os.environ.get('DATABASE') or os.environ.get('database')
            if database is not None and ('?' in database or '@' in database):
                engine_kwargs.setdefault('connect_args', {})['database'] = database
            url = '{}{}://{}{}{}{}'.format(
                dialect, '' if driver is None else '+{}'.format(driver),
                '' if user is None else '{}{}@'.format(user, '' if password is None else ':{}'.format(password)),
                '' if host is None else host, '' if port is None else ':{}'.format(port),
                '' if database is None or '?' in database or '@' in database else '/{}'.format(database))
        self.dialect = dialect
        self.driver = driver
        if is_pool:
            engine_kwargs['pool_size'] = pool_size
        else:
            engine_kwargs['poolclass'] = NullPool
        if charset == '':
            charset = {'mysql': 'utf8mb4', 'postgresql': None}.get(dialect, 'utf8')
        if charset is not None:
            kwargs['charset'] = charset
        engine_kwargs.setdefault('execution_options', {})['autocommit'] = autocommit
        if kwargs:
            if engine_kwargs.get('connect_args'):
                engine_kwargs['connect_args'].update(kwargs)
            else:
                engine_kwargs['connect_args'] = kwargs
        self.origin_result = origin_result
        self.dataset = dataset
        self._transactions = []
        if statement_save_data is None:
            statement_save_data = 'REPLACE' if dialect == 'mysql' else 'INSERT INTO'
        if escape_auto_format is None:  # postgresql, oracle如果escape字段则区分大小写，故当前仅mysql设默认escape
            escape_auto_format = dialect == 'mysql'
        if escape_formatter is None:
            escape_formatter = {'mysql': '`{}`', 'postgresql': '"{}"', 'oracle': '"{}"', 'sqlite': '"{}"',
                                'mssql': '[{}]'}.get(dialect, '{}')
        self.engine = create_engine(url, **engine_kwargs)
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         keep_args_as_dict, transform_formatter, try_times_connect, time_sleep_connect, raise_error)

    def query(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
              not_one_by_one: bool = True, auto_format: bool = False, keys: Union[str, Collection[str], None] = None,
              commit: Optional[bool] = None, escape_auto_format: Optional[bool] = None,
              escape_formatter: Optional[str] = None, empty_string_to_none: Optional[bool] = None,
              keep_args_as_dict: Optional[bool] = None, transform_formatter: Optional[bool] = None,
              try_times_connect: Union[int, float, None] = None, time_sleep_connect: Union[int, float, None] = None,
              raise_error: Optional[bool] = None, origin_result: Optional[bool] = None, dataset: Optional[bool] = None
              ) -> Union[int, tuple, list, RecordCollection, tablib.Dataset]:
        # sqlalchemy无cursor；sqlalchemy不支持位置参数；增加origin_result, dataset参数
        # args 支持单条记录: list/tuple/dict 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True或keys不为None: 注意此时query会被format一次；keep_args_as_dict强制视为True；
        #                                首条记录需为dict（not_one_by_one=False时所有记录均需为dict），或者含除自增字段外所有字段并按顺序排好各字段值，或者自行传入keys
        # fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        args, is_multiple = self.standardize_args(args, None, empty_string_to_none,
                                                  auto_format or keys is not None or keep_args_as_dict, True)
        if not args:
            return self.try_execute(query, args, fetchall, dictionary, False, commit, try_times_connect,
                                    time_sleep_connect, raise_error, origin_result=origin_result, dataset=dataset)
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
                        arg, dict) else '', ','.join(map(':{}'.format, arg) if isinstance(arg, dict) else ('%s',) * len(
                        arg)))
                else:
                    if isinstance(keys, str):
                        keys = tuple(key.strip() for key in keys.split(','))
                    query = query.format('({})'.format(','.join((escape_formatter.format(
                        key) for key in keys) if escape_auto_format else keys)), ','.join(map(
                        ':{}'.format, keys) if isinstance(args[0] if is_multiple else args, dict) else ('%s',) * len(
                        keys)))
            return self.try_execute(query, args, fetchall, dictionary, is_multiple, commit, try_times_connect,
                                    time_sleep_connect, raise_error, origin_result=origin_result, dataset=dataset)
        # 依次执行
        ori_query = query
        result = [] if fetchall else 0
        if auto_format or keys is not None:
            if escape_formatter is None:
                escape_formatter = self.escape_formatter
            if keys is not None:
                if isinstance(keys, str):
                    keys = tuple(key.strip() for key in keys.split(','))
                query = query.format('({})'.format(','.join((escape_formatter.format(
                    key) for key in keys) if escape_auto_format else keys)), ','.join(map(
                    ':{}'.format, keys) if isinstance(args[0], dict) else ('%s',) * len(keys)))
        for arg in args:
            if auto_format and keys is None:
                query = ori_query.format('({})'.format(','.join((escape_formatter.format(
                    key) for key in arg) if escape_auto_format else map(str, arg))) if isinstance(
                    arg, dict) else '', ','.join(map(':{}'.format, arg) if isinstance(arg, dict) else ('%s',) * len(
                    arg)))
            temp_result = self.try_execute(query, arg, fetchall, dictionary, not_one_by_one, commit, try_times_connect,
                                           time_sleep_connect, raise_error, origin_result=origin_result,
                                           dataset=dataset)
            if fetchall:
                result.append(temp_result)
            else:
                result += temp_result
        return result

    def select_to_try(self, table: Optional[str] = None, num: Union[int, str, None] = 1,
                      key_fields: Union[str, Iterable[str]] = 'id', extra_fields: Union[str, Iterable[str], None] = '',
                      tried: Union[int, str, None] = 0, tried_after: Union[int, str, None] = 1,
                      tried_field: Optional[str] = 'is_tried', finished: Union[int, str, None] = None,
                      finished_field: Optional[str] = 'is_finished', plus_1_field: Optional[str] = '',
                      dictionary: Optional[bool] = None, autocommit_after: Optional[bool] = None,
                      select_where: Optional[str] = None, select_extra: str = '', update_set: Optional[str] = None,
                      set_extra: Optional[str] = '', update_where: Optional[str] = None, update_extra: str = '',
                      try_times_connect: Union[int, float, None] = None,
                      time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                      origin_result: Optional[bool] = None, dataset: Optional[bool] = None
                      ) -> Union[int, tuple, list, RecordCollection, tablib.Dataset]:
        # sqlalchemy事务使用不同；增加origin_result, dataset参数
        # key_fields: update一句where部分使用, extra_fields: 不在update一句使用, return结果包含key_fields和extra_fields
        # finished_field: select一句 where finished_field=finished，finished设为None则取消
        # plus_1_field: update一句令其+=1
        # select_where: 不为None则替换select一句的where部分(为''时删除where)
        # update_set: 不为None则替换update一句的set部分
        # update_where: 不为None则替换update一句的where部分
        if table is None:
            table = self.table
        if not isinstance(key_fields, str):
            key_fields = ','.join(key_fields)
        if extra_fields is not None and not isinstance(extra_fields, str):
            extra_fields = ','.join(extra_fields)
        if select_where is None:
            select_where = ' where {}={}{}'.format(tried_field, tried, '' if finished is None else ' and {}={}'.format(
                finished_field, finished))
        elif select_where:
            if not select_where.startswith(' where'):
                select_where = ' where ' + select_where.lstrip(' ')
            elif select_where.startswith('where'):
                select_where = ' ' + select_where
        query = 'select {}{} from {}{}{}{}'.format(key_fields, ',' + extra_fields if extra_fields else '', table,
                                                   select_where, select_extra, ' limit {}'.format(num) if num else '')
        transaction = self.begin()
        result = self.query(query, fetchall=True, dictionary=dictionary, commit=False, transform_formatter=False,
                            try_times_connect=try_times_connect, time_sleep_connect=time_sleep_connect,
                            raise_error=raise_error, origin_result=origin_result, dataset=dataset)
        if not result:
            self.commit(transaction)
            if autocommit_after is not None:
                self.autocommit = autocommit_after
            return result
        if update_where is None:
            if dictionary and not origin_result and not dataset:
                update_where = ' or '.join(' and '.join('{}={}'.format(key, row[key]) for key in key_fields.split(
                    ',')) for row in result)
            else:
                update_where = ' or '.join(' and '.join('{}={}'.format(key, row[i]) for i, key in enumerate(
                    key_fields.split(','))) for row in result)
        elif update_where.startswith('where'):
            update_where = update_where[5:].lstrip(' ')
        elif update_where.startswith(' where'):
            update_where = update_where[6:].lstrip(' ')
        query = 'update {} set {} where {}{}'.format(table, '{}{}'.format(' and '.join(filter(None, ('{}={}'.format(
            tried_field, tried_after) if tried_after is not None and tried_after != tried else '', '{0}={0}+1'.format(
            plus_1_field) if plus_1_field else ''))), set_extra) if update_set is None else update_set, update_where,
                                                     update_extra)
        is_success = self.query(query, fetchall=False, commit=False, transform_formatter=False,
                                try_times_connect=try_times_connect, time_sleep_connect=time_sleep_connect,
                                raise_error=raise_error)
        if is_success:
            self.commit(transaction)
        else:
            result = ()
            self.rollback(transaction)
        if autocommit_after is not None:
            self.autocommit = autocommit_after
        return result

    def close(self, try_close: bool = True) -> None:
        if try_close:
            try:
                self.connection.close()
            except Exception as e:
                if self.log:
                    self.logger.error('{}: {}  (in try_close)'.format(str(type(e))[8:-2], e), exc_info=True)
            try:
                self.engine.dispose()
            except Exception as e:
                if self.log:
                    self.logger.error('{}: {}  (in try_close)'.format(str(type(e))[8:-2], e), exc_info=True)
        else:
            self.connection.close()
            self.engine.dispose()

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool):
        if hasattr(self, 'connection') and value != self._autocommit:
            isolation_level = 'AUTOCOMMIT' if value else self.connection.default_isolation_level
            self.engine.update_execution_options(isolation_level=isolation_level)
            self.connection = self.connection.execution_options(isolation_level=isolation_level)
        self._autocommit = value

    @contextlib.contextmanager
    def transaction(self):
        # return: transaction
        transaction = self.begin()
        try:
            yield transaction
            transaction.commit()
        except Exception:
            transaction.rollback()

    def begin(self) -> Union[RootTransaction, Transaction]:
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()
        transaction = self.connection.begin()
        self._transactions.append(transaction)
        return transaction

    def commit(self, transaction=None) -> None:
        if transaction is not None:
            transaction.commit()
        elif self._transactions:
            self._transactions[-1].commit()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def rollback(self, transaction=None) -> None:
        if transaction is not None:
            transaction.rollback()
        elif self._transactions:
            self._transactions[-1].rollback()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def connect(self) -> None:
        self.connection = self.engine.connect()

    def try_execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                    many: bool = False, commit: Optional[bool] = None,
                    try_times_connect: Union[int, float, None] = None,
                    time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                    cursor: None = None, origin_result: Optional[bool] = None, dataset: Optional[bool] = None
                    ) -> Union[int, tuple, list, RecordCollection, tablib.Dataset]:
        # sqlalchemy无cursor；增加origin_result, dataset参数
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        if try_times_connect is None:
            try_times_connect = self.try_times_connect
        if time_sleep_connect is None:
            time_sleep_connect = self.time_sleep_connect
        if raise_error is None:
            raise_error = self.raise_error
        self.set_connection()
        try_count_connect = 0
        while True:
            try:
                return self.execute(query, args, fetchall, dictionary, many, commit, cursor, origin_result, dataset)
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  {}'.format(
                            str(type(e))[8:-2], try_count_connect, e, self._query_log_text(query, args)), exc_info=True)
                    if raise_error:
                        raise e
                    break
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  {}'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e,
                        self._query_log_text(query, args)), exc_info=True)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                self.rollback()
                if self.log:
                    self.logger.error('{}: {}  {}'.format(
                        str(type(e))[8:-2], e, self._query_log_text(query, args)), exc_info=True)
                if raise_error:
                    raise e
                break
        if fetchall:
            return ()
        return 0

    def execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                many: bool = False, commit: Optional[bool] = None, cursor: None = None,
                origin_result: Optional[bool] = None, dataset: Optional[bool] = None
                ) -> Union[int, tuple, list, RecordCollection, tablib.Dataset]:
        # 覆盖调用逻辑；增加origin_result, dataset参数
        # fetchall=False: return成功执行语句数(many模式按数据条数)
        if dictionary is None:
            dictionary = self.dictionary
        if origin_result is None:
            origin_result = self.origin_result
        if dataset is None:
            dataset = self.dataset
        self.set_connection()
        if args is None:
            cursor = self.connection.execute(text(query))
        elif not many:
            if isinstance(args, dict):
                cursor = self.connection.execute(text(query), **args)
            else:
                cursor = self.connection.execute(text(query % args))
        else:
            cursor = self.connection.execute(text(query), *args)
        if commit and not self._autocommit:
            self.commit()
        if not fetchall:
            return len(args) if many and hasattr(args, '__len__') else 1
        if origin_result:
            return list(cursor)
        result = RecordCollection((Record(cursor.keys(), row) for row in cursor) if cursor.returns_rows else iter(()))
        if dataset:
            return result.dataset
        if dictionary:
            return result.all(as_dict=True)
        return result

    def ping(self) -> None:
        # sqlalchemy没有ping
        self.set_connection()

    def format(self, query: str, args: Any, raise_error: Optional[bool] = None) -> str:
        # sqlalchemy没有literal和escape，暂不借鉴mysql实现
        try:
            if args is None:
                return query
            return query % args if '%' in query else query.format(args)
        except Exception as e:
            if raise_error or raise_error is None and self.raise_error:
                raise e
            return query

    def _before_query_and_get_cursor(self, fetchall: bool = True, dictionary: Optional[bool] = None):
        # sqlalchemy无cursor，不使用该方法，替代以直接调用set_connection
        raise NotImplementedError

    def call_proc(self, name: str, args: Iterable = (), fetchall: bool = True, dictionary: Optional[bool] = None,
                  commit: Optional[bool] = None, empty_string_to_none: Optional[bool] = None,
                  try_times_connect: Union[int, float, None] = None, time_sleep_connect: Union[int, float, None] = None,
                  raise_error: Optional[bool] = None, origin_result: Optional[bool] = None,
                  dataset: Optional[bool] = None) -> Union[int, tuple, list, RecordCollection, tablib.Dataset]:
        # sqlalchemy以直接execute执行存储过程；增加origin_result, dataset参数
        # 执行存储过程
        # name: 存储过程名
        # args: 存储过程参数(可以为None)
        # fetchall=False: return成功执行数(1)
        query = '{}{}'.format(name, '({})'.format(','.join(args)) if args else '')
        return self.query(query, None, fetchall, dictionary, True, False, commit, try_times_connect, time_sleep_connect,
                          raise_error, origin_result=origin_result, dataset=dataset)

    def query_file(self, path: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                   not_one_by_one: bool = True, auto_format: bool = False,
                   keys: Union[str, Collection[str], None] = None, commit: Optional[bool] = None,
                   escape_auto_format: Optional[bool] = None, escape_formatter: Optional[str] = None,
                   empty_string_to_none: Optional[bool] = None, keep_args_as_dict: Optional[bool] = None,
                   transform_formatter: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                   time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                   origin_result: Optional[bool] = None, dataset: Optional[bool] = None
                   ) -> Union[int, tuple, list, RecordCollection, tablib.Dataset]:
        with open(path) as f:
            query = f.read()
        return self.query(query, args, fetchall, dictionary, not_one_by_one, auto_format, keys, commit,
                          escape_auto_format, escape_formatter, empty_string_to_none, keep_args_as_dict,
                          transform_formatter, try_times_connect, time_sleep_connect, raise_error, origin_result,
                          dataset)

    def get_table_names(self) -> List[str]:
        """Returns a list of table names for the connected database."""

        # Setup SQLAlchemy for Database inspection.
        return inspect(self.engine).get_table_names()
