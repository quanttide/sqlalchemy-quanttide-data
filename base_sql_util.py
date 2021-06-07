import time
import contextlib


class SqlUtil(object):
    lib = None

    # lib.ProgrammingError: close
    # lib.InterfaceError, self.lib.OperationalError: ping, try_connect, try_execute, call_proc
    # lib.cursors.DictCursor: query, call_proc
    # lib.connect: connect
    def __init__(self, host, port=None, user=None, password=None, database=None, charset=None, autocommit=True,
                 connect_now=True, log=True, table=None, statement_save_data='INSERT INTO', dictionary=True,
                 escape_auto_format=False, escape_formatter='{}', empty_string_to_none=True, keep_args_as_dict=False,
                 try_times_connect=3, time_sleep_connect=3, raise_error=False):
        self.host = host
        self.port = port
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
        self.keep_args_as_dict = keep_args_as_dict
        self.try_times_connect = try_times_connect
        self.time_sleep_connect = time_sleep_connect
        self.raise_error = raise_error
        self.log = log
        if log:
            import logging
            self.logger = logging.getLogger(__name__)
        if connect_now:
            self.try_connect()

    def query(self, query, args=None, fetchall=True, dictionary=None, not_one_by_one=True, auto_format=False,
              commit=None, try_times_connect=None, time_sleep_connect=None, raise_error=None, empty_string_to_none=None,
              keep_args_as_dict=None, escape_auto_format=None, escape_formatter=None):
        # args 支持单条记录: list/tuple/dict 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 注意此时query会被format一次；首条记录需为dict（not_one_by_one=False时所有记录均需为dict），或者含除自增字段外所有字段并按顺序排好各字段值
        # fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        args, is_multiple = self.standardize_args(args, None, empty_string_to_none, keep_args_as_dict, True)
        if not args:
            return self.try_execute(query, args, fetchall, dictionary, False, commit, try_times_connect,
                                    time_sleep_connect, raise_error)
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
                                    time_sleep_connect, raise_error)
        # 依次执行
        ori_query = query
        result = [] if fetchall else 0
        if auto_format and escape_formatter is None:
            escape_formatter = self.escape_formatter
        cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        for arg in args:
            if auto_format:
                query = ori_query.format('({})'.format(','.join((escape_formatter.format(
                    key) for key in arg) if escape_auto_format else map(str, arg))) if isinstance(
                    arg, dict) else '', ','.join(('%s',) * len(arg)))
            temp_result = self.try_execute(query, arg, fetchall, dictionary, not_one_by_one, commit, try_times_connect,
                                           time_sleep_connect, raise_error, cursor)
            if fetchall:
                result.append(temp_result)
            else:
                result += temp_result
        cursor.close()
        return result

    def save_data(self, args, table=None, statement=None, extra=None, not_one_by_one=False, auto_format=True, key=None,
                  commit=None, try_times_connect=None, time_sleep_connect=None, raise_error=None,
                  escape_auto_format=None, empty_string_to_none=None, keep_args_as_dict=None):
        # data_list 支持单条记录: list/tuple/dict，或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 首条记录需为dict（one_by_one=True时所有记录均需为dict），或者含除自增字段外所有字段并按顺序排好各字段值
        # auto_format=False: 数据需按%s,...格式传入key并按顺序排好各字段值，或者按%(name)s,...格式传入key并且所有记录均为dict
        # 默认not_one_by_one=False: 为了部分记录无法插入时能够单独跳过这些记录（有log）
        # fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        if not args and args != 0:
            return 0
        query = '{} {}{} VALUES({{}}){}'.format(
            self.statement_save_data if statement is None else statement, self.table if table is None else table,
            '{}' if auto_format else '' if key is None else '({})'.format(key),
            ' {}'.format(extra) if extra is not None else '')
        return self.query(query, args, False, False, not_one_by_one, auto_format, commit, try_times_connect,
                          time_sleep_connect, raise_error, escape_auto_format, empty_string_to_none, keep_args_as_dict)

    def select_to_try(self, table=None, num=1, key_fields='id', extra_fields='', tried=0, tried_after=1,
                      tried_field='is_tried', finished=0, finished_field='is_finished', plus_1_field='',
                      autocommit_after=True, select_where=None, select_extra='', update_set=None, set_extra='',
                      update_where=None, update_extra='', try_times_connect=None, time_sleep_connect=None,
                      raise_error=None):
        # key_fields: update一句where部分使用
        # extra_fields: 不在update一句使用, return结果包含key_fields和extra_fields
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
        self.begin()
        result = self.query(query, None, True, False, True, False, False, try_times_connect, time_sleep_connect,
                            raise_error)
        if not result:
            self.commit()
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
            self.commit()
        else:
            result = ()
            self.rollback()
        if autocommit_after is not None:
            self.autocommit = autocommit_after
        return result

    def finish(self, result, table=None, key_fields='', finished=1, finished_field='is_finished', commit=True,
               update_where=None, update_extra='', try_times_connect=None, time_sleep_connect=None, raise_error=None):
        # 对key_fields对应result的记录，set finished_field=finished
        # key_fields为''或None时，result需为dict或list[dict]，key_fields取result的keys
        # update_where: 不为None则替换update一句的where部分
        result = self.standardize_args(result, True, False, True, True)
        if not result:
            return
        if table is None:
            table = self.table
        if not key_fields:
            key_fields = tuple(result[0].keys())
        elif isinstance(key_fields, str):
            key_fields = key_fields.split(',')
        if update_where is None:
            if isinstance(result[0], dict):
                update_where = ' or '.join(' and '.join('{}={}'.format(key, row[key]) for key in key_fields)
                                           for row in result)
            else:
                update_where = ' or '.join(' and '.join('{}={}'.format(key, row[i]) for i, key in enumerate(key_fields))
                                           for row in result)
        elif update_where.startswith('where'):
            update_where = update_where[5:].lstrip(' ')
        elif update_where.startswith(' where'):
            update_where = update_where[6:].lstrip(' ')
        query = 'update {} set {}={} where {}{}'.format(table, finished_field, finished, update_where, update_extra)
        return self.query(query, None, False, False, True, False, commit, try_times_connect, time_sleep_connect,
                          raise_error)

    def close(self, try_close=True):
        if try_close:
            try:
                self.connection.close()
            except self.lib.ProgrammingError:
                # _mysql_exceptions.ProgrammingError: closing a closed connection
                pass
        else:
            self.connection.close()

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
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

    def begin(self):
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()
        self.connection.begin()

    def commit(self):
        self.connection.commit()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def rollback(self):
        if hasattr(self, 'connection'):
            self.connection.rollback()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def connect(self):
        self.connection = self.lib.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.database, charset=self.charset, autocommit=self._autocommit)

    def set_connection(self):
        if not hasattr(self, 'connection'):
            self.try_connect()

    def try_connect(self, try_times_connect=None, time_sleep_connect=None, raise_error=None):
        if try_times_connect is None:
            try_times_connect = self.try_times_connect
        if time_sleep_connect is None:
            time_sleep_connect = self.time_sleep_connect
        if raise_error is None:
            raise_error = self.raise_error
        try_count_connect = 0
        while True:
            try:
                self.connect()
                return
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  (in try_connect)'.format(
                            str(type(e))[8:-2], try_count_connect, e), exc_info=True)
                    if raise_error:
                        raise e
                    return
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  (in try_connect)'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e), exc_info=True)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                if self.log:
                    self.logger.error('{}: {}  (in try_connect)'.format(str(type(e))[8:-2], e), exc_info=True)
                if raise_error:
                    raise e
                return

    def try_execute(self, query, args=None, fetchall=True, dictionary=None, many=False, commit=None,
                    try_times_connect=None, time_sleep_connect=None, raise_error=None, cursor=None):
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
                            str(type(e))[8:-2], try_count_connect, e, self._query_log_text(query, args)), exc_info=True)
                    if raise_error:
                        if ori_cursor is None:
                            cursor.close()
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
                    if ori_cursor is None:
                        cursor.close()
                    raise e
                break
        if fetchall:
            return ()
        return 0

    def execute(self, query, args=None, fetchall=True, dictionary=None, many=False, commit=None, cursor=None):
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        if not many:
            cursor.execute(query, args)
        else:  # executemany: 一句插入多条记录，当语句超出1024000字符时拆分成多个语句；传单条记录需用列表包起来
            cursor.executemany(query, args)
        if commit and not self._autocommit:
            self.commit()
        result = cursor.fetchall() if fetchall else len(args) if many and hasattr(args, '__len__') else 1
        if ori_cursor is None:
            cursor.close()
        return result

    def standardize_args(self, args, to_multiple=None, empty_string_to_none=None, keep_args_as_dict=None,
                         get_is_multiple=False):
        if not args and args != 0:
            return args if not get_is_multiple else (args, False)
        if not hasattr(args, '__getitem__'):
            if hasattr(args, '__iter__'):  # set, Generator, range
                args = tuple(args)
                if not args:
                    return args if not get_is_multiple else (args, False)
            else:  # int, etc.
                args = (args,)
        elif isinstance(args, str):
            args = (args,)
        # else: dict, list, tuple, dataset/row, recordcollection/record
        if to_multiple is None:
            to_multiple = not isinstance(args, dict) and not isinstance(args[0], str) and (
                    hasattr(args[0], '__getitem__') or hasattr(args[0], '__iter__'))
        if empty_string_to_none is None:
            empty_string_to_none = self.empty_string_to_none
        if keep_args_as_dict is None:
            keep_args_as_dict = self.keep_args_as_dict
        if not to_multiple:
            if isinstance(args, dict):
                if not keep_args_as_dict:
                    args = tuple(each if each != '' else None for each in args.values()
                                 ) if empty_string_to_none else tuple(args.values())
                elif empty_string_to_none:
                    args = {key: value if value != '' else None for key, value in args.items()}
            elif empty_string_to_none:
                args = tuple(each if each != '' else None for each in args)
        else:
            if isinstance(args, dict):
                if keep_args_as_dict:
                    args = ({key: value if value != '' else None for key, value in args.items()},
                            ) if empty_string_to_none else (args,)
                else:
                    args = (tuple(each if each != '' else None for each in args.values()),
                            ) if empty_string_to_none else (tuple(args.values()),)
            elif isinstance(args[0], dict):
                if not keep_args_as_dict:
                    args = tuple(tuple(e if e != '' else None for e in each.values()) for each in args
                                 ) if empty_string_to_none else tuple(tuple(each.values()) for each in args)
                elif empty_string_to_none:
                    args = tuple({key: value if value != '' else None for key, value in each.items()} for each in args)
            elif isinstance(args[0], str):
                args = (tuple(each if each != '' else None for each in args),) if empty_string_to_none else (args,)
            elif not hasattr(args[0], '__getitem__'):
                if hasattr(args[0], '__iter__'):  # list[set, Generator, range]
                    # mysqlclient, pymysql均只支持dict, list, tuple，不支持set, Generator等
                    args = tuple(tuple(e if e != '' else None for e in each) for each in args
                                 ) if empty_string_to_none else tuple(tuple(each) for each in args)
                else:  # list[int, etc.]
                    args = (tuple(each if each != '' else None for each in args),) if empty_string_to_none else (args,)
            elif empty_string_to_none:
                args = tuple(tuple(e if e != '' else None for e in each) for each in args)
        return args if not get_is_multiple else (args, to_multiple)

    def ping(self):
        try:
            self.connection.ping()
        except (self.lib.InterfaceError, self.lib.OperationalError):
            # MySQLdb._exceptions.OperationalError: (2013, 'Lost connection to MySQL server during query')
            # MySQLdb._exceptions.OperationalError: (2006, 'MySQL server has gone away')
            # _mysql_exceptions.InterfaceError: (0, '')
            self.close()
            self.try_connect()
        except AttributeError:
            # AttributeError: 'SqlUtil' object has no attribute 'connection'
            # AttributeError: 'NoneType' object has no attribute 'ping'
            self.try_connect()

    def format(self, query, args, raise_error=True):
        try:
            if args is None:
                return query
            if isinstance(args, dict):
                new_args = dict((key, self.connection.literal(item)) for key, item in args.items())
                return query % new_args if '%' in query else query.format(**new_args)
            new_args = tuple(map(self.connection.literal, args))
            return query % new_args if '%' in query else query.format(*new_args)
        except Exception as e:
            if raise_error:
                raise e
            return

    def _before_query_and_get_cursor(self, fetchall=True, dictionary=None):
        if fetchall and (self.dictionary if dictionary is None else dictionary):
            cursor_class = self.lib.cursors.DictCursor
        else:
            cursor_class = None
        self.set_connection()
        return self.connection.cursor(cursor_class)

    def _query_log_text(self, query, args):
        try:
            return 'formatted_query: {}'.format(self.format(query, args, True))
        except Exception as e:
            return 'query: {}  args: {}  {}: {}'.format(query, args, str(type(e))[8:-2], e)

    def call_proc(self, name, args=(), fetchall=True, dictionary=None, commit=None, try_times_connect=None,
                  time_sleep_connect=None, raise_error=None, empty_string_to_none=None):
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
