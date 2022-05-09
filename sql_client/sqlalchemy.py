# -*- coding: utf-8 -*-

import os
import time
import contextlib
from typing import Any, Union, Optional, Tuple, List, Iterable, Collection, Generator

import tablib
import sqlalchemy

from .base import SqlClient as BaseSqlClient, Paramstyle, NOTSET, Notset
from ._records import RecordCollection, Record


class SqlClient(BaseSqlClient):
    lib = sqlalchemy.exc

    def __init__(self, dialect: Optional[str] = None, driver: Optional[str] = None, host: Optional[str] = None,
                 port: Union[int, str, None] = None, user: Optional[str] = None, password: Optional[str] = None,
                 database: Optional[str] = None, charset: Optional[str] = NOTSET, autocommit: bool = True,
                 connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: Optional[str] = None, dictionary: bool = False,
                 escape_auto_format: Optional[bool] = None, escape_formatter: Optional[str] = None,
                 empty_string_to_none: bool = True, args_to_dict: Optional[bool] = None,
                 to_paramstyle: Optional[Paramstyle] = Paramstyle.named, try_times_connect: Union[int, float] = 3,
                 time_sleep_connect: Union[int, float] = 3, raise_error: bool = False, origin_result: bool = False,
                 dataset: bool = False, is_pool: bool = False, pool_size: int = 1, engine_kwargs: Optional[dict] = None,
                 **kwargs):
        # dialect也可输入完整url；或者将完整url存于环境变量：DATABASE_URL
        # 完整url格式：dialect[+driver]://user:password@host/dbname[?key=value..]
        # 对user和password影响sqlalchemy解析url的字符进行转义(sqlalchemy解析完url会对user和password解转义) (若从dialect或环境变量传入整个url，需提前转义好)
        # sqlalchemy不会对database进行解转义，故database含?时需移至engine_kwargs['connect_args']['database']
        # sqlalchemy 1.3: database含@时也需移至engine_kwargs['connect_args']['database']
        # 优先级: dictionary > origin_result > dataset
        if engine_kwargs is None:
            engine_kwargs = {}
        if dialect is None:
            dialect = os.environ.get('DB_DIALECT') or os.environ.get('DATABASE_URL') or os.environ.get(
                'DB_DATABASE_URL') or os.environ.get('DB_URL')
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
                    driver = os.environ.get('DB_DRIVER')
                if driver is not None:
                    driver = driver.lower()
            if dialect == 'sqlserver':
                dialect = 'mssql'
            if host is None:
                host = os.environ.get('DB_HOST')
            if port is None:
                port = os.environ.get('DB_PORT')
            if user is None:
                user = os.environ.get('DB_USER')
            if user is not None:
                user = user.replace('%', '%25').replace(':', '%3A').replace('/', '%2F')
            if password is None:
                password = os.environ.get('DB_PASSWORD')
            if password is not None:
                password = password.replace('%', '%25').replace('@', '%40')
            if database is None:
                database = os.environ.get('DB_DATABASE')
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
            engine_kwargs['poolclass'] = sqlalchemy.pool.NullPool
        if charset is NOTSET:
            charset = {'mysql': 'utf8mb4', 'postgresql': None, 'sqlite': None}.get(dialect, 'utf8')
        if charset is not None:
            kwargs['charset' if dialect != 'oracle' else 'encoding'] = charset
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
        self.url = url
        self.engine_kwargs = engine_kwargs
        self.create_engine()
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         args_to_dict, to_paramstyle, try_times_connect, time_sleep_connect, raise_error)

    def query(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
              chunksize: Optional[int] = None, not_one_by_one: bool = True, auto_format: bool = False,
              keys: Union[str, Collection[str], None] = None, commit: Optional[bool] = None,
              escape_auto_format: Optional[bool] = None, escape_formatter: Optional[str] = None,
              empty_string_to_none: Optional[bool] = None, args_to_dict: Union[bool, Notset, None] = NOTSET,
              to_paramstyle: Union[Paramstyle, Notset, None] = NOTSET, keep_cursor: Optional[bool] = False,
              cursor: None = None, try_times_connect: Union[int, float, None] = None,
              time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
              origin_result: Optional[bool] = None, dataset: Optional[bool] = None
              ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                               tablib.Dataset, Generator],
                         Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                                     tablib.Dataset, Generator], sqlalchemy.engine.ResultProxy]]:
        # sqlalchemy无cursor；增加origin_result, dataset参数
        # args 支持单条记录: list/tuple/dict, 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 注意此时query会被format一次; args_to_dict视为False;
        #                   首条记录需为dict(not_one_by_one=False时所有记录均需为dict), 或者含除自增字段外所有字段并按顺序排好各字段值, 或者自行传入keys
        # fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        # args_to_dict=None: 不做dict和list之间转换; args_to_dict=False: dict强制转为list; args_to_dict=NOTSET: 读取默认配置
        # keep_cursor: 返回(result, cursor), 并且不自动关闭cursor;
        #              如果args为多条记录且not_one_by_one=False且设置了chunksize且fetchall=True(仅此情况会使用多个cursor), 则只会保留最后一个cursor
        if args and not hasattr(args, '__getitem__') and hasattr(args, '__iter__'):  # set, Generator, range
            args = tuple(args)
        if not args and args not in (0, ''):
            return self.try_execute(query, args, fetchall, dictionary, chunksize, False, commit, keep_cursor, cursor,
                                    try_times_connect, time_sleep_connect, raise_error, origin_result, dataset)
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
            return self.try_execute(query, args, fetchall, dictionary, chunksize, is_multiple, commit, keep_cursor,
                                    cursor, try_times_connect, time_sleep_connect, raise_error, origin_result, dataset)
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
        for arg in args:
            if auto_format and keys is None:
                query = ori_query.format('({})'.format(','.join(map(escape_formatter.format if escape_auto_format else
                                                                    str, arg)))
                                         if isinstance(arg, dict) and not is_key_generated else '',
                                         ','.join(self.paramstyle_formatter(arg, to_paramstyle)))
            temp_result = self.try_execute(query, arg, fetchall, dictionary, chunksize, not_one_by_one, commit,
                                           keep_cursor, cursor, try_times_connect, time_sleep_connect, raise_error,
                                           origin_result, dataset)
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
        return result

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
                      origin_result: Optional[bool] = None, dataset: Optional[bool] = None
                      ) -> Union[int, tuple, list, RecordCollection, tablib.Dataset]:
        # sqlalchemy事务使用不同；增加origin_result, dataset参数
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
                            time_sleep_connect=time_sleep_connect, raise_error=raise_error, origin_result=origin_result,
                            dataset=dataset)
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

    def begin(self) -> Union[sqlalchemy.engine.RootTransaction, sqlalchemy.engine.Transaction]:
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

    def create_engine(self) -> None:
        self.engine = sqlalchemy.create_engine(self.url, **self.engine_kwargs)

    def try_execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                    chunksize: Optional[int] = None, many: bool = False, commit: Optional[bool] = None,
                    keep_cursor: Optional[bool] = False, cursor: None = None,
                    try_times_connect: Union[int, float, None] = None,
                    time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                    origin_result: Optional[bool] = None, dataset: Optional[bool] = None
                    ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                                     tablib.Dataset, Generator],
                               Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                                           tablib.Dataset, Generator], sqlalchemy.engine.ResultProxy]]:
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
                return self.execute(query, args, fetchall, dictionary, chunksize, many, commit, keep_cursor, cursor,
                                    origin_result, dataset)
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
                chunksize: Optional[int] = None, many: bool = False, commit: Optional[bool] = None,
                keep_cursor: Optional[bool] = False, cursor: None = None, origin_result: Optional[bool] = None,
                dataset: Optional[bool] = None
                ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                                 tablib.Dataset, Generator],
                           Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                                       tablib.Dataset, Generator], sqlalchemy.engine.ResultProxy]]:
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
            cursor = self.connection.execute(sqlalchemy.text(query))
        elif not many:
            if isinstance(args, dict):
                cursor = self.connection.execute(sqlalchemy.text(query), **args)
            else:
                cursor = self.connection.execute(sqlalchemy.text(query % args))
        else:
            cursor = self.connection.execute(sqlalchemy.text(query), *args)
        if commit and not self._autocommit:
            self.commit()
        if not fetchall:
            result = len(args) if many and hasattr(args, '__len__') else 1
        elif origin_result and not dictionary:
            result = (list(cursor) if chunksize is None else map(list, self._fetchmany_generator(
                cursor, chunksize, keep_cursor))) if cursor.returns_rows else []
        elif chunksize is not None and cursor.returns_rows:
            if dictionary:
                result = (RecordCollection([Record(cursor.keys(), row) for row in result]).all(as_dict=True) for result
                          in self._fetchmany_generator(cursor, chunksize, keep_cursor))
            elif dataset:
                result = (RecordCollection([Record(cursor.keys(), row) for row in result]).dataset for result in
                          self._fetchmany_generator(cursor, chunksize, keep_cursor))
            else:
                result = (RecordCollection([Record(cursor.keys(), row) for row in result]) for result in
                          self._fetchmany_generator(cursor, chunksize, keep_cursor))
        else:
            result = RecordCollection([Record(cursor.keys(), row) for row in cursor] if cursor.returns_rows else [])
            if dictionary:
                result = result.all(as_dict=True)
            elif dataset:
                result = result.dataset
        if keep_cursor:
            return result, cursor
        if chunksize is None or not fetchall:
            cursor.close()
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

    def query_file(self, path: str, encoding: Optional[str] = None, args: Any = None, fetchall: bool = True,
                   dictionary: Optional[bool] = None, chunksize: Optional[int] = None, not_one_by_one: bool = True,
                   auto_format: bool = False, keys: Union[str, Collection[str], None] = None,
                   commit: Optional[bool] = None, escape_auto_format: Optional[bool] = None,
                   escape_formatter: Optional[str] = None, empty_string_to_none: Optional[bool] = None,
                   args_to_dict: Union[bool, Notset, None] = NOTSET,
                   to_paramstyle: Union[Paramstyle, Notset, None] = NOTSET, keep_cursor: Optional[bool] = False,
                   cursor: None = None, try_times_connect: Union[int, float, None] = None,
                   time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                   origin_result: Optional[bool] = None, dataset: Optional[bool] = None
                   ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                                    tablib.Dataset, Generator],
                              Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                                          tablib.Dataset, Generator], sqlalchemy.engine.ResultProxy]]:
        with open(path, encoding=encoding) as f:
            query = f.read()
        return self.query(query, args, fetchall, dictionary, chunksize, not_one_by_one, auto_format, keys, commit,
                          escape_auto_format, escape_formatter, empty_string_to_none, args_to_dict, to_paramstyle,
                          keep_cursor, cursor, try_times_connect, time_sleep_connect, raise_error, origin_result,
                          dataset)

    def call_proc(self, name: str, args: Iterable = (), fetchall: bool = True, dictionary: Optional[bool] = None,
                  chunksize: Optional[int] = None, commit: Optional[bool] = None,
                  empty_string_to_none: Optional[bool] = None, keep_cursor: Optional[bool] = False, cursor: None = None,
                  try_times_connect: Union[int, float, None] = None, time_sleep_connect: Union[int, float, None] = None,
                  raise_error: Optional[bool] = None, origin_result: Optional[bool] = None,
                  dataset: Optional[bool] = None
                  ) -> Union[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                                   tablib.Dataset, Generator],
                             Tuple[Union[int, list, tuple, Tuple[Union[tuple, list, dict, Any]], RecordCollection,
                                         tablib.Dataset, Generator], sqlalchemy.engine.ResultProxy]]:
        # sqlalchemy以直接execute执行存储过程；增加origin_result, dataset参数
        # 执行存储过程
        # name: 存储过程名
        # args: 存储过程参数(可以为None)
        # fetchall=False: return成功执行数(1)
        # keep_cursor: 返回(result, cursor), 并且不自动关闭cursor
        query = '{}{}'.format(name, '({})'.format(','.join(args)) if args else '')
        return self.query(query, fetchall=fetchall, dictionary=dictionary, chunksize=chunksize, commit=commit,
                          keep_cursor=keep_cursor, cursor=cursor, try_times_connect=try_times_connect,
                          time_sleep_connect=time_sleep_connect, raise_error=raise_error, origin_result=origin_result,
                          dataset=dataset)

    def get_table_names(self) -> List[str]:
        """Returns a list of table names for the connected database."""

        # Setup SQLAlchemy for Database inspection.
        return sqlalchemy.inspect(self.engine).get_table_names()
