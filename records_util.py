import time
import contextlib

import records
import sqlalchemy.pool.impl
import sqlalchemy.exc
import sqlalchemy.sql.expression

import base_sql_util


class SqlUtil(base_sql_util.SqlUtil):
    lib = sqlalchemy.exc

    def __init__(self, dialect=None, driver=None, host=None, port=None, user=None, password=None, database=None,
                 charset=None, autocommit=True, connect_now=True, log=True, table=None,
                 statement_save_data='INSERT INTO', dictionary=False, escape_auto_format=None, escape_formatter=None,
                 empty_string_to_none=True, keep_args_as_dict=False, try_times_connect=3, time_sleep_connect=3,
                 raise_error=False, origin_result=False, dataset=False, is_pool=False, pool_size=1, engine_kwargs=None,
                 **kwargs):
        # dialect也可输入完整url；或者将完整url存于环境变量：DATABASE_URL
        # 完整url格式：dialect[+driver]://user:password@host/dbname[?key=value..]
        # 优先级: dictionary > origin_result > dataset
        url = dialect if host is None else '{}{}://{}:{}@{}{}{}'.format(
            dialect, '' if driver is None else '+{}'.format(driver), user, password, host,
            '' if port is None else ':{}'.format(port), '' if database is None else '/{}'.format(database))
        if engine_kwargs is None:
            engine_kwargs = {}
        if is_pool:
            engine_kwargs['pool_size'] = pool_size
        else:
            engine_kwargs['poolclass'] = sqlalchemy.pool.impl.NullPool
        if charset is not None:
            kwargs['charset'] = charset
        engine_kwargs.setdefault('execution_options', {})['autocommit'] = autocommit
        engine_kwargs['connect_args'] = kwargs
        self.origin_result = origin_result
        self.dataset = dataset
        self._transactions = []
        self.engine = records.Database(url, **engine_kwargs)
        self.origin_engine = self.engine._engine
        if escape_auto_format is None:  # postgresql, oracle如果escape字段则区分大小写，故当前仅mysql设默认escape
            escape_auto_format = self.engine.db_url.lower().startswith('mysql')
        if escape_formatter is None:
            lower_url = self.engine.db_url.lower()
            if lower_url.startswith('mysql'):
                escape_formatter = '`{}`'
            elif lower_url.startswith(('postgresql', 'oracle', 'sqlite')):
                escape_formatter = '"{}"'
            elif lower_url.startswith('sqlserver'):
                escape_formatter = '[{}]'
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         keep_args_as_dict, try_times_connect, time_sleep_connect, raise_error)

    def query(self, query, args=None, fetchall=True, dictionary=None, not_one_by_one=True, auto_format=False,
              commit=None, try_times_connect=None, time_sleep_connect=None, raise_error=None, empty_string_to_none=None,
              keep_args_as_dict=None, escape_auto_format=None, escape_formatter=None, origin_result=None, dataset=None):
        # sqlalchemy无cursor；增加origin_result, dataset参数
        # args 支持单条记录: list/tuple/dict 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 注意此时query会被format一次；首条记录需为dict（not_one_by_one=False时所有记录均需为dict），或者含除自增字段外所有字段并按顺序排好各字段值
        # fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        args, is_multiple = self.standardize_args(args, None, empty_string_to_none, keep_args_as_dict, True)
        if not args:
            return self.try_execute(query, args, fetchall, dictionary, False, commit, try_times_connect,
                                    time_sleep_connect, raise_error, origin_result=origin_result, dataset=dataset)
        if escape_auto_format is None:
            escape_auto_format = self.escape_auto_format
        if not is_multiple or not_one_by_one:  # 执行一次
            if auto_format:
                if escape_formatter is None:
                    escape_formatter = self.escape_formatter
                arg = args[0] if is_multiple else args
                query = query.format('({})'.format(','.join((escape_formatter.format(
                    key) for key in arg) if escape_auto_format else map(str, arg))) if isinstance(
                    arg, dict) else '', ','.join(('%s',) * len(arg)))
            return self.try_execute(query, args, fetchall, dictionary, is_multiple, commit, try_times_connect,
                                    time_sleep_connect, raise_error, origin_result=origin_result, dataset=dataset)
        # 依次执行
        ori_query = query
        result = [] if fetchall else 0
        if auto_format and escape_formatter is None:
            escape_formatter = self.escape_formatter
        for i, arg in enumerate(args):
            if auto_format:
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
                      tried_field='is_tried', finished=0, finished_field='is_finished', plus_1_field='',
                      autocommit_after=True, select_where=None, select_extra='', update_set=None, set_extra='',
                      update_where=None, update_extra='', try_times_connect=None, time_sleep_connect=None,
                      raise_error=None):
        # sqlalchemy事务使用不同；采用origin_result=True
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
        result = self.query(query, None, True, False, True, False, False, try_times_connect, time_sleep_connect,
                            raise_error, origin_result=True)
        if not result:
            self.commit(transaction)
            if autocommit_after is not None:
                self.autocommit = autocommit_after
            return result
        if update_where is None:
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
        is_success = self.query(query, None, False, False, True, False, False, try_times_connect, time_sleep_connect,
                                raise_error)
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
                self.engine.close()
            except Exception as e:
                if self.log:
                    self.logger.error('{}: {}  (in try_close)'.format(str(type(e))[8:-2], e), exc_info=True)
        else:
            self.connection.close()
            self.engine.close()

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        if hasattr(self, 'connection') and value != self._autocommit:
            self.origin_engine.update_execution_options(autocommit=value)
            self.origin_connection = self.origin_connection.execution_options(autocommit=value)
            self.connection._conn = self.origin_connection
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
        transaction = self.origin_connection.begin()
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
        self.connection = self.engine.get_connection()
        self.origin_connection = self.connection._conn

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
            cursor = self.origin_connection.execute(sqlalchemy.sql.expression.text(query))
        elif not many:
            if isinstance(args, dict):
                cursor = self.origin_connection.execute(sqlalchemy.sql.expression.text(query), **args)
            else:
                cursor = self.origin_connection.execute(sqlalchemy.sql.expression.text(query % args))
        else:
            cursor = self.origin_connection.execute(sqlalchemy.sql.expression.text(query), *args)
        if commit and not self._autocommit:
            self.commit()
        if not fetchall:
            return len(args) if many and hasattr(args, '__len__') else 1
        if origin_result and not dictionary:
            return list(cursor)
        result = records.RecordCollection((records.Record(cursor.keys(), row) for row in cursor)
                                          if cursor.returns_rows else iter(()))
        if dictionary:
            return result.all(as_dict=True)
        if dataset:
            return result.dataset
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
        return self.engine.get_table_names()
