# -*- coding: utf-8 -*-

import os
import time
import contextlib
import re
from collections import OrderedDict
from inspect import isclass
from typing import Optional, Union

import tablib
from sqlalchemy.pool.impl import NullPool
from sqlalchemy import exc
from sqlalchemy.sql.expression import text
from sqlalchemy.engine import create_engine
from sqlalchemy.inspection import inspect

from sql_client.base import SqlClient as BaseSqlClient


class SqlClient(BaseSqlClient):
    lib = exc

    def __init__(self, dialect: Optional[str] = None, driver: Optional[str] = None, host: Optional[str] = None,
                 port: Union[int, str, None] = None, user: Optional[str] = None, password: Optional[str] = None,
                 database: Optional[str] = None, charset: Optional[str] = None, autocommit: bool = True,
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

    def query(self, query, args=None, fetchall=True, dictionary=None, not_one_by_one=True, auto_format=False, keys=None,
              commit=None, escape_auto_format=None, escape_formatter=None, empty_string_to_none=None,
              keep_args_as_dict=None, transform_formatter=None, try_times_connect=None, time_sleep_connect=None,
              raise_error=None, origin_result=None, dataset=None):
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
                        arg, dict) else '', ','.join(('%s',) * len(arg)))
                elif isinstance(keys, str):
                    query = query.format('({})'.format(','.join(escape_formatter.format(key.strip()) for key in
                                                                keys.split(',')) if escape_auto_format else keys),
                                         ','.join(('%s',) * (keys.count(',') + 1)))
                else:
                    query = query.format('({})'.format(','.join((escape_formatter.format(
                        key) for key in keys) if escape_auto_format else keys)), ','.join(('%s',) * len(keys)))
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
                    query = query.format('({})'.format(','.join(escape_formatter.format(key.strip()) for key in
                                                                keys.split(',')) if escape_auto_format else keys),
                                         ','.join(('%s',) * (keys.count(',') + 1)))
                else:
                    query = query.format('({})'.format(','.join((escape_formatter.format(
                        key) for key in keys) if escape_auto_format else keys)), ','.join(('%s',) * len(keys)))
        for arg in args:
            if auto_format and keys is None:
                query = ori_query.format('({})'.format(','.join((escape_formatter.format(
                    key) for key in arg) if escape_auto_format else map(str, arg))) if isinstance(
                    arg, dict) else '', ','.join(('%s',) * len(arg)))
            temp_result = self.try_execute(query, arg, fetchall, dictionary, not_one_by_one, commit, try_times_connect,
                                           time_sleep_connect, raise_error, origin_result=origin_result,
                                           dataset=dataset)
            if fetchall:
                result.append(temp_result)
            else:
                result += temp_result
        return result

    def select_to_try(self, table=None, num=1, key_fields='id', extra_fields='', tried=0, tried_after=1,
                      tried_field='is_tried', finished=None, finished_field='is_finished', plus_1_field='',
                      dictionary=None, autocommit_after=None, select_where=None, select_extra='', update_set=None,
                      set_extra='', update_where=None, update_extra='', try_times_connect=None, time_sleep_connect=None,
                      raise_error=None, origin_result=None, dataset=None):
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

    def close(self, try_close=True):
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
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
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

    def begin(self):
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()
        transaction = self.connection.begin()
        self._transactions.append(transaction)
        return transaction

    def commit(self, transaction=None):
        if transaction is not None:
            transaction.commit()
        elif self._transactions:
            self._transactions[-1].commit()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def rollback(self, transaction=None):
        if transaction is not None:
            transaction.rollback()
        elif self._transactions:
            self._transactions[-1].rollback()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def connect(self):
        self.connection = self.engine.connect()

    def try_execute(self, query, args=None, fetchall=True, dictionary=None, many=False, commit=None,
                    try_times_connect=None, time_sleep_connect=None, raise_error=None, cursor=None, origin_result=None,
                    dataset=None):
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

    def execute(self, query, args=None, fetchall=True, dictionary=None, many=False, commit=None, cursor=None,
                origin_result=None, dataset=None):
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

    def ping(self):
        # sqlalchemy没有ping
        self.set_connection()

    def format(self, query, args, raise_error=True):
        # sqlalchemy没有literal和escape，暂不借鉴mysql实现
        try:
            if args is None:
                return query
            return query % args if '%' in query else query.format(args)
        except Exception as e:
            if raise_error:
                raise e
            return

    def _before_query_and_get_cursor(self, fetchall=True, dictionary=None):
        # sqlalchemy无cursor，不使用该方法，替代以直接调用set_connection
        raise NotImplementedError

    def call_proc(self, name, args=(), fetchall=True, dictionary=None, commit=None, try_times_connect=None,
                  time_sleep_connect=None, raise_error=None, empty_string_to_none=None, origin_result=None,
                  dataset=None):
        # sqlalchemy以直接execute执行存储过程；增加origin_result, dataset参数
        # 执行存储过程
        # name: 存储过程名
        # args: 存储过程参数(可以为None)
        # fetchall=False: return成功执行数(1)
        query = '{}{}'.format(name, '({})'.format(','.join(args)) if args else '')
        return self.query(query, None, fetchall, dictionary, True, False, commit, try_times_connect, time_sleep_connect,
                          raise_error, origin_result=origin_result, dataset=dataset)

    def query_file(self, path, args=None, fetchall=True, dictionary=None, not_one_by_one=True, auto_format=False,
                   commit=None, try_times_connect=None, time_sleep_connect=None, raise_error=None,
                   empty_string_to_none=None, keep_args_as_dict=None, escape_auto_format=None, escape_formatter=None,
                   origin_result=None, dataset=None):
        with open(path) as f:
            query = f.read()
        return self.query(query, args, fetchall, dictionary, not_one_by_one, auto_format, commit, try_times_connect,
                          time_sleep_connect, raise_error, empty_string_to_none, keep_args_as_dict, escape_auto_format,
                          escape_formatter, origin_result, dataset)

    def get_table_names(self):
        """Returns a list of table names for the connected database."""

        # Setup SQLAlchemy for Database inspection.
        return inspect(self.engine).get_table_names()


class Record(object):
    """A row, from a query, from a database."""
    __slots__ = ('_keys', '_values')

    def __init__(self, keys, values):
        self._keys = keys
        self._values = values

        # Ensure that lengths match properly.
        assert len(self._keys) == len(self._values)

    def keys(self):
        """Returns the list of column names from the query."""
        return self._keys

    def values(self):
        """Returns the list of values from the query."""
        return self._values

    def __repr__(self):
        return '<Record {}>'.format(self.export('json')[1:-1])

    def __getitem__(self, key):
        # Support for index-based lookup.
        if isinstance(key, int):
            return self.values()[key]

        # Support for string-based lookup.
        if key in self.keys():
            i = self.keys().index(key)
            if self.keys().count(key) > 1:
                raise KeyError("Record contains multiple '{}' fields.".format(key))
            return self.values()[i]

        raise KeyError("Record contains no '{}' field.".format(key))

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __dir__(self):
        standard = dir(super(Record, self))
        # Merge standard attrs with generated ones (from column names).
        return sorted(standard + [str(k) for k in self.keys()])

    def get(self, key, default=None):
        """Returns the value for a given key, or default."""
        try:
            return self[key]
        except KeyError:
            return default

    def as_dict(self, ordered=False):
        """Returns the row as a dictionary, as ordered."""
        items = zip(self.keys(), self.values())

        return OrderedDict(items) if ordered else dict(items)

    @property
    def dataset(self):
        """A Tablib Dataset containing the row."""
        data = tablib.Dataset()
        data.headers = self.keys()

        row = _reduce_datetimes(self.values())
        data.append(row)

        return data

    def export(self, format, **kwargs):
        """Exports the row to the given format."""
        return self.dataset.export(format, **kwargs)


class RecordCollection(object):
    """A set of excellent Records from a query."""

    def __init__(self, rows):
        self._rows = rows
        self._all_rows = []
        self.pending = True

    def __repr__(self):
        return '<RecordCollection size={} pending={}>'.format(len(self), self.pending)

    def __iter__(self):
        """Iterate over all rows, consuming the underlying generator
        only when necessary."""
        i = 0
        while True:
            # Other code may have iterated between yields,
            # so always check the cache.
            if i < len(self):
                yield self[i]
            else:
                # Throws StopIteration when done.
                # Prevent StopIteration bubbling from generator, following https://www.python.org/dev/peps/pep-0479/
                try:
                    yield next(self)
                except StopIteration:
                    return
            i += 1

    def next(self):
        return self.__next__()

    def __next__(self):
        try:
            nextrow = next(self._rows)
            self._all_rows.append(nextrow)
            return nextrow
        except StopIteration:
            self.pending = False
            raise StopIteration('RecordCollection contains no more rows.')

    def __getitem__(self, key):
        is_int = isinstance(key, int)

        # Convert RecordCollection[1] into slice.
        if is_int:
            key = slice(key, key + 1)

        while len(self) < key.stop or key.stop is None:
            try:
                next(self)
            except StopIteration:
                break

        rows = self._all_rows[key]
        if is_int:
            return rows[0]
        else:
            return RecordCollection(iter(rows))

    def __len__(self):
        return len(self._all_rows)

    def export(self, format, **kwargs):
        """Export the RecordCollection to a given format (courtesy of Tablib)."""
        return self.dataset.export(format, **kwargs)

    @property
    def dataset(self):
        """A Tablib Dataset representation of the RecordCollection."""
        # Create a new Tablib Dataset.
        data = tablib.Dataset()

        # If the RecordCollection is empty, just return the empty set
        # Check number of rows by typecasting to list
        if len(list(self)) == 0:
            return data

        # Set the column names as headers on Tablib Dataset.
        first = self[0]

        data.headers = first.keys()
        for row in self.all():
            row = _reduce_datetimes(row.values())
            data.append(row)

        return data

    def all(self, as_dict=False, as_ordereddict=False):
        """Returns a list of all rows for the RecordCollection. If they haven't
        been fetched yet, consume the iterator and cache the results."""

        # By calling list it calls the __iter__ method
        rows = list(self)

        if as_dict:
            return [r.as_dict() for r in rows]
        elif as_ordereddict:
            return [r.as_dict(ordered=True) for r in rows]

        return rows

    def as_dict(self, ordered=False):
        return self.all(as_dict=not ordered, as_ordereddict=ordered)

    def first(self, default=None, as_dict=False, as_ordereddict=False):
        """Returns a single record for the RecordCollection, or `default`. If
        `default` is an instance or subclass of Exception, then raise it
        instead of returning it."""

        # Try to get a record, or return/raise default.
        try:
            record = self[0]
        except IndexError:
            if isexception(default):
                raise default
            return default

        # Cast and return.
        if as_dict:
            return record.as_dict()
        elif as_ordereddict:
            return record.as_dict(ordered=True)
        else:
            return record

    def one(self, default=None, as_dict=False, as_ordereddict=False):
        """Returns a single record for the RecordCollection, ensuring that it
        is the only record, or returns `default`. If `default` is an instance
        or subclass of Exception, then raise it instead of returning it."""

        # Ensure that we don't have more than one row.
        try:
            self[1]
        except IndexError:
            return self.first(default=default, as_dict=as_dict, as_ordereddict=as_ordereddict)
        else:
            raise ValueError('RecordCollection contained more than one row. '
                             'Expects only one row when using '
                             'RecordCollection.one')

    def scalar(self, default=None):
        """Returns the first column of the first row, or `default`."""
        row = self.one()
        return row[0] if row else default


def isexception(obj):
    """Given an object, return a boolean indicating whether it is an instance
    or subclass of :py:class:`Exception`.
    """
    if isinstance(obj, Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False


def _reduce_datetimes(row):
    """Receives a row, converts datetimes to strings."""

    row = list(row)

    for i in range(len(row)):
        if hasattr(row[i], 'isoformat'):
            row[i] = row[i].isoformat()
    return tuple(row)
