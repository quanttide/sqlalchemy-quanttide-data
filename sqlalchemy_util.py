import time
import contextlib
import os
from collections import OrderedDict
from inspect import isclass

import tablib
import sqlalchemy.pool.impl
import sqlalchemy.exc
import sqlalchemy.sql.expression
import sqlalchemy.engine.create
import sqlalchemy.inspection

import base_sql_util


class SqlUtil(base_sql_util.SqlUtil):
    def __init__(self, dialect=None, driver=None, host=None, port=None, user=None, password=None, database=None,
                 charset=None, autocommit=True, connect_now=True, dictionary=False, origin_result=False, dataset=False,
                 log=True, is_pool=False, pool_size=1, engine_kwargs=None, **kwargs):
        # dialect也可输入完整url；或者将完整url存于环境变量：DATABASE_URL
        # 完整url格式：dialect[+driver]://user:password@host/dbname[?key=value..]
        # 优先级: dictionary > origin_result > dataset
        # 注意许多方法在base_sql_util的基础上有增减参数
        url = os.environ.get(
            'DATABASE_URL') if dialect is None else dialect if host is None else '{}{}://{}:{}@{}{}{}'.format(
            dialect, '' if driver is None else '+{}'.format(driver), user, password, host,
            '' if port is None else ':{}'.format(port), '' if database is None else '/{}'.format(database))
        self.lib = sqlalchemy.exc
        if engine_kwargs is None:
            engine_kwargs = {}
        if is_pool:
            engine_kwargs['pool_size'] = pool_size
        else:
            engine_kwargs['poolclass'] = sqlalchemy.pool.impl.NullPool
        if charset is not None:
            kwargs['charset'] = charset
        engine_kwargs.setdefault('execution_options', {})['autocommit'] = autocommit
        self._autocommit = autocommit
        self.temp_autocommit = None
        self.dictionary = dictionary
        self.origin_result = origin_result
        self.dataset = dataset
        self._transactions = []
        self.log = log
        if log:
            import logging
            self.logger = logging.getLogger(__name__)
        engine_kwargs['connect_args'] = kwargs
        self.engine = sqlalchemy.engine.create.create_engine(url, **engine_kwargs)
        if connect_now:
            self.try_connect()

    def connect(self):
        self.connection = self.engine.connect()

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
            self.engine.update_execution_options(autocommit=value)
            self.connection = self.connection.execution_options(autocommit=value)
        self._autocommit = value

    @contextlib.contextmanager
    def transaction(self):
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

    def format(self, query, args, raise_error=True):
        try:
            if args is None:
                return query
            return query % args if '%' in query else query.format(args)
        except Exception as e:
            if raise_error:
                raise e
            return

    def execute(self, query, values=None, fetchall=True, dictionary=None, origin_result=None, dataset=None, many=False,
                commit=True):
        if dictionary is None:
            dictionary = self.dictionary
        if origin_result is None:
            origin_result = self.origin_result
        if dataset is None:
            dataset = self.dataset
        self.set_connection()
        if values is None:
            cursor = self.connection.execute(sqlalchemy.sql.expression.text(query))
        elif not many:
            if isinstance(values, dict):
                cursor = self.connection.execute(sqlalchemy.sql.expression.text(query), **values)
            else:
                cursor = self.connection.execute(sqlalchemy.sql.expression.text(query % values))
        else:
            cursor = self.connection.execute(sqlalchemy.sql.expression.text(query), *values)
        if origin_result and fetchall and not dictionary:
            return list(cursor)
        result = RecordCollection((Record(cursor.keys(), row) for row in cursor) if cursor.returns_rows else iter(()))
        if commit and not self._autocommit:
            self.commit()
        if not fetchall:
            return 1 if not many else len(values)
        elif dictionary:
            return result.all(as_dict=True)
        elif dataset:
            return result.dataset
        return result

    def try_execute(self, query, values=None, args=None, fetchall=True, dictionary=None, origin_result=None,
                    dataset=None, many=False, commit=True, try_times_connect=3, time_sleep_connect=3,
                    raise_error=False):
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        # values可以为None
        self.set_connection()
        try_count_connect = 0
        while True:
            try:
                return self.execute(query, values, fetchall, dictionary, origin_result, dataset, many, commit)
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  args: {}  {}'.format(
                            str(type(e))[8:-2], try_count_connect, e, args, self._query_log_text(query, values)),
                            exc_info=True)
                    if raise_error:
                        raise e
                    break
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  args: {}  {}'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e, args,
                        self._query_log_text(query, values)), exc_info=True)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                self.rollback()
                if self.log:
                    self.logger.error('{}: {}  args: {}  {}'.format(
                        str(type(e))[8:-2], e, args, self._query_log_text(query, values)), exc_info=True)
                if raise_error:
                    raise e
                break
        if fetchall:
            return ()
        return 0

    @staticmethod
    def _auto_format_query(query, arg, escape_auto_format, escape_punc="`"):
        return query.format('({})'.format(','.join(('{1}{0}{1}'.format(
            key, escape_punc) for key in arg) if escape_auto_format else map(str, arg))) if isinstance(
            arg, dict) else '', ','.join(('%s',) * len(arg)))  # postgresql不使用``而使用""

    def query(self, query, args=None, fetchall=True, dictionary=None, origin_result=None, dataset=None, many=True,
              commit=True, auto_format=False, escape_auto_format=False, escape_punc="`", empty_string_to_none=True,
              keep_args_as_dict=False, try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # postgresql如果用双引号escape字段则区分大小写，故默认escape_auto_format=False
        # args 支持单条记录: list/tuple/dict 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 注意此时query会被format一次；首条记录需为dict（many=False时所有记录均需为dict），或者含除自增字段外所有字段并按顺序排好各字段值
        # fetchall=False: return成功执行语句数(many模式按数据条数)
        is_args_list = isinstance(args, (list, tuple)) and isinstance(args[0], (
            dict, list, tuple, set))  # set在standardize_args中转为list
        values = self.standardize_args(args, is_args_list, empty_string_to_none,
                                       keep_args_as_dict and not auto_format)  # many=True但单条记录的不套列表，预备转非many模式
        if not is_args_list or many:  # 执行一次
            if auto_format:
                arg = args[0] if is_args_list else args
                query = self._auto_format_query(query, arg, escape_auto_format, escape_punc)
            return self.try_execute(query, values, args, fetchall, dictionary, origin_result, dataset, is_args_list,
                                    commit, try_times_connect, time_sleep_connect,
                                    raise_error)  # many=True但单条记录的转非many模式
        # 依次执行
        ori_query = query
        result = [] if fetchall else 0
        for i, value in enumerate(values):
            if auto_format:
                query = self._auto_format_query(ori_query, args[i], escape_auto_format, escape_punc)
            temp_result = self.try_execute(query, value, args[i], fetchall, dictionary, origin_result, dataset, many,
                                           commit, try_times_connect, time_sleep_connect, raise_error)
            if fetchall:
                result.append(temp_result)
            else:
                result += temp_result
        return result

    def save_data(self, data_list=None, table=None, statement='INSERT INTO', extra=None, many=False, auto_format=True,
                  key=None, escape_auto_format=True, escape_punc="`", empty_string_to_none=True,
                  keep_args_as_dict=False, try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # data_list 支持单条记录: list/tuple/dict 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 首条记录需为dict（many=False时所有记录均需为dict），或者含除自增字段外所有字段并按顺序排好各字段值; auto_format=False: 数据需按%s,...格式传入key并按顺序排好各字段值，或者按%(name)s,...格式传入key并且所有记录均为dict
        # 默认many=False: 为了部分记录无法插入时能够单独跳过这些记录（有log）
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        # def query(self, query, args=None, fetchall=True, dictionary=None, many=True, commit=True, auto_format=False,
        #           escape_auto_format=False, escape_punc="`", empty_string_to_none=True, keep_args_as_dict=False,
        #           try_times_connect=3, time_sleep_connect=3, raise_error=False):
        if not data_list:
            return 0
        query = '{} {}{} VALUES({{}}){}'.format(
            statement, table, '{}' if auto_format else '' if key is None else '({})'.format(key),
            ' {}'.format(extra) if extra is not None else '')
        return self.query(query, data_list, False, False, False, False, many, True, auto_format, escape_auto_format,
                          escape_punc, empty_string_to_none, keep_args_as_dict, try_times_connect, time_sleep_connect,
                          raise_error)

    def select_to_try(self, table, num=1, key_field='id', extra_field='', tried=0, tried_after=1,
                      tried_field='is_tried', finished=None, finished_after=None, finished_field='is_finished',
                      select_where=None, select_extra='', update_set=None, set_extra='', update_where=None,
                      update_extra='', try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # key_field: update一句where部分使用, extra_field: 不在update一句使用, return结果包含key_field和extra_field
        # select_where: 不为None则替换select一句的where部分（需以 空格where 开头）
        # update_set: 不为None则替换update一句的set部分（不需set和空格）
        # update_where: 不为None则替换update一句的where部分（需以 空格where 开头）
        transaction = self.begin()
        query = 'select {}{} from {}{}{}'.format(
            key_field, ',' + extra_field if extra_field else '', table, ' where {}={}{}{}'.format(
                tried_field, tried, '' if finished is None else ' and {}={}'.format(finished_field, finished),
                select_extra) if select_where is None else select_where,
            '' if num is None or num == 0 else ' limit {}'.format(num))
        result = self.try_execute(query, None, None, True, False, False, False, False, False, try_times_connect,
                                  time_sleep_connect, raise_error)
        if not result:
            self.commit(transaction)
            return result
        if update_where is None:
            if num == 1:
                if ',' in key_field:
                    update_where = ' where {}'.format(' and '.join('{}={}'.format(
                        key, result[0][i]) for i, key in enumerate(key_field.split(','))))
                else:
                    update_where = ' where {}={}'.format(key_field, result[0])
            else:
                if ',' in key_field:
                    update_where = ' where {}'.format(' or '.join(' and '.join('{}={}'.format(
                        key, row[i]) for i, key in enumerate(key_field.split(',')))) for row in result)
                else:
                    update_where = ' where {}'.format(' or '.join('{}={}'.format(key_field, row[0])) for row in result)
        query = 'update {} set {}{}{}'.format(
            table, '{}={}{}{}'.format(tried_field, tried_after, '' if finished_after is None else ' and {}={}'.format(
                finished_field, finished_after), set_extra) if update_set is None else update_set, update_where,
            update_extra)
        temp_result = self.try_execute(query, None, None, False, False, False, False, False, False, try_times_connect,
                                       time_sleep_connect, raise_error)
        if not temp_result:
            self.rollback(transaction)
            return ()
        self.commit(transaction)
        return result

    def query_file(self, path, args=None, fetchall=True, dictionary=None, dataset=None, builtin_list=None, many=True,
                   commit=True, auto_format=False, escape_auto_format=False, escape_punc="`", empty_string_to_none=True,
                   keep_args_as_dict=False, try_times_connect=3, time_sleep_connect=3, raise_error=False):
        with open(path) as f:
            query = f.read()
        return self.query(query, args, fetchall, dictionary, dataset, builtin_list, many, commit, auto_format,
                          escape_auto_format, escape_punc, empty_string_to_none, keep_args_as_dict, try_times_connect,
                          time_sleep_connect, raise_error)

    def get_table_names(self):
        """Returns a list of table names for the connected database."""

        # Setup SQLAlchemy for Database inspection.
        return sqlalchemy.inspection.inspect(self.engine).get_table_names()


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
