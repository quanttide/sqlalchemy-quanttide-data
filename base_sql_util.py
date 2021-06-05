import time
import contextlib


class SqlUtil(object):
    # self.lib.ProgrammingError: close
    # self.lib.InterfaceError, self.lib.OperationalError: ping, try_connect, try_execute, call_proc
    # self.lib.cursors.DictCursor: query, call_proc
    # self.lib.connect: connect
    # self.connection.autocommit: autocommit.setter
    def __init__(self, host, port=None, user=None, password=None, database=None, charset='utf8mb4', autocommit=True,
                 connect_now=True, dictionary=True, log=True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self._autocommit = autocommit
        self.temp_autocommit = None
        self.dictionary = dictionary
        self.log = log
        if log:
            import logging
            self.logger = logging.getLogger(__name__)
        if connect_now:
            self.try_connect()

    def query(self, query, args=None, fetchall=True, dictionary=None, many=True, commit=True, auto_format=False,
              escape_auto_format=True, empty_string_to_none=True, keep_args_as_dict=False, try_times_connect=3,
              time_sleep_connect=3, raise_error=False):
        # args 支持单条记录: list/tuple/dict 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 注意此时query会被format一次；首条记录需为dict（many=False时所有记录均需为dict），或者含除自增字段外所有字段并按顺序排好各字段值
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        is_args_list = isinstance(args, (list, tuple)) and isinstance(args[0], (
            dict, list, tuple, set))  # set在standardize_args中转为list
        values = self.standardize_args(args, is_args_list, empty_string_to_none,
                                       keep_args_as_dict and not auto_format)  # many=True但单条记录的不套列表，预备转非many模式
        if not is_args_list or many:  # 执行一次
            if auto_format:
                arg = args[0] if is_args_list else args
                query = self._auto_format_query(query, arg, escape_auto_format)
            return self.try_execute(query, values, args, None, fetchall, dictionary, is_args_list, commit,
                                    try_times_connect, time_sleep_connect, raise_error)  # many=True但单条记录的转非many模式
        # 依次执行
        ori_query = query
        result = [] if fetchall else 0
        cursor = self._before_query_and_get_cursor(fetchall, self.dictionary if dictionary is None else dictionary)
        for i, value in enumerate(values):
            if auto_format:
                query = self._auto_format_query(ori_query, args[i], escape_auto_format)
            temp_result = self.try_execute(query, value, args[i], cursor, fetchall, dictionary, many, commit,
                                           try_times_connect, time_sleep_connect, raise_error)
            if fetchall:
                result.append(temp_result)
            else:
                result += temp_result
        cursor.close()
        return result

    def save_data(self, data_list=None, table=None, statement='REPLACE', extra=None, many=False, auto_format=True,
                  key=None, escape_auto_format=True, empty_string_to_none=True, keep_args_as_dict=False,
                  try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # data_list 支持单条记录: list/tuple/dict 或多条记录: list/tuple/set[list/tuple/dict]
        # auto_format=True: 首条记录需为dict（many=False时所有记录均需为dict），或者含除自增字段外所有字段并按顺序排好各字段值; auto_format=False: 数据需按%s,...格式传入key并按顺序排好各字段值，或者按%(name)s,...格式传入key并且所有记录均为dict
        # 默认many=False: 为了部分记录无法插入时能够单独跳过这些记录（有log）
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        # def query(self, query, args=None, fetchall=True, dictionary=True, many=True, commit=True, auto_format=False,
        #           escape_auto_format=True, empty_string_to_none=True, keep_args_as_dict=False, try_times_connect=3,
        #           time_sleep_connect=3, raise_error=False):
        if not data_list:
            return 0
        query = '{} {}{} VALUES({{}}){}'.format(
            statement, table, '{}' if auto_format else '' if key is None else '({})'.format(key),
            ' {}'.format(extra) if extra is not None else '')
        return self.query(query, data_list, False, False, many, True, auto_format, escape_auto_format,
                          empty_string_to_none, keep_args_as_dict, try_times_connect, time_sleep_connect, raise_error)

    def select_to_try(self, table, num=1, key_field='id', extra_field='', tried=0, tried_after=1,
                      tried_field='is_tried', finished=None, finished_after=None, finished_field='is_finished',
                      select_where=None, select_extra='', update_set=None, set_extra='', update_where=None,
                      update_extra='', try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # key_field: update一句where部分使用, extra_field: 不在update一句使用, return结果包含key_field和extra_field
        # select_where: 不为None则替换select一句的where部分（需以 空格where 开头）
        # update_set: 不为None则替换update一句的set部分（不需set和空格）
        # update_where: 不为None则替换update一句的where部分（需以 空格where 开头）
        self.begin()
        query = 'select {}{} from {}{}{}'.format(
            key_field, ',' + extra_field if extra_field else '', table, ' where {}={}{}{}'.format(
                tried_field, tried, '' if finished is None else ' and {}={}'.format(finished_field, finished),
                select_extra) if select_where is None else select_where,
            '' if num is None or num == 0 else ' limit {}'.format(num))
        result = self.try_execute(query, None, None, None, True, False, False, False, try_times_connect,
                                  time_sleep_connect, raise_error)
        if not result:
            self.commit()
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
        temp_result = self.try_execute(query, None, None, None, False, False, False, False, try_times_connect,
                                       time_sleep_connect, raise_error)
        if not temp_result:
            self.rollback()
            return ()
        self.commit()
        return result

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

    def try_connect(self, try_times_connect=3, time_sleep_connect=3, raise_error=False):
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

    def try_execute(self, query, values=None, args=None, cursor=None, fetchall=True, dictionary=None, many=False,
                    commit=True, try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        # values可以为None
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, self.dictionary if dictionary is None else dictionary)
        try_count_connect = 0
        while True:
            try:
                result = self.execute(query, values, fetchall, dictionary, many, commit, cursor)
                if ori_cursor is None:
                    cursor.close()
                return result
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  args: {}  {}'.format(
                            str(type(e))[8:-2], try_count_connect, e, args, self._query_log_text(query, values)),
                            exc_info=True)
                    if raise_error:
                        if ori_cursor is None:
                            cursor.close()
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
                    if ori_cursor is None:
                        cursor.close()
                    raise e
                break
        if fetchall:
            return ()
        return 0

    def execute(self, query, values=None, fetchall=True, dictionary=None, many=False, commit=True, cursor=None):
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, self.dictionary if dictionary is None else dictionary)
        if not many:
            cursor.execute(query, values)
        else:  # executemany: 一句插入多条记录，当语句超出1024000字符时拆分成多个语句；传单条记录需用列表包起来
            cursor.executemany(query, values)
        if commit and not self._autocommit:
            self.commit()
        result = cursor.fetchall() if fetchall else 1 if not many else len(values)
        if ori_cursor is None:
            cursor.close()
        return result

    @staticmethod
    def standardize_args(args, keep_args_list=False, empty_string_to_none=True, keep_args_as_dict=False):
        if args is None:
            return args
        if not keep_args_list:
            if isinstance(args, dict) and not keep_args_as_dict:
                args = tuple(args.values())
            if empty_string_to_none and hasattr(args, '__iter__'):
                if isinstance(args, dict) and keep_args_as_dict:
                    args = {key: value if value != '' else None for key, value in args.items()}
                else:
                    args = tuple(each if each != '' else None for each in args)
        else:
            if not isinstance(args, (
                    dict, list, tuple)):  # 已知mysqlclient, pymysql均只支持dict, list, tuple，不支持set, Generator等
                args = list(args)
            if isinstance(args, dict) or not isinstance(args[0], (dict, list, tuple)):
                args = [args]
            if isinstance(args[0], dict) and not keep_args_as_dict:
                args = [tuple(each.values()) for each in args]
            if empty_string_to_none:
                if isinstance(args[0], dict) and keep_args_as_dict:
                    args = [{key: value if value != '' else None for key, value in each.items()} for each in args]
                else:
                    args = [tuple(e if e != '' else None for e in each) for each in args]
        return args

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

    def try_format(self, query, args):
        try:
            return self.format(query, args, True)
        except Exception as e:
            return '{}: {}: {}'.format(query, str(type(e))[8:-2], e)

    @staticmethod
    def _auto_format_query(query, arg, escape_auto_format):
        return query.format('({})'.format(','.join(('`{}`'.format(key) for key in arg) if escape_auto_format else map(
            str, arg))) if isinstance(arg, dict) else '', ','.join(('%s',) * len(arg)))  # sqlserver不使用``而使用[]

    def _before_query_and_get_cursor(self, fetchall, dictionary):
        if dictionary and fetchall:
            cursor_class = self.lib.cursors.DictCursor
        else:
            cursor_class = None
        self.set_connection()
        return self.connection.cursor(cursor_class)

    def _query_log_text(self, query, values):
        return 'formatted_query: {}'.format(self.try_format(query, values))

    def call_proc(self, name, args=(), fetchall=True, dictionary=None, commit=True, empty_string_to_none=True,
                  try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # 执行存储过程
        # name: 存储过程名
        # args: 存储过程参数(不能为None，要可迭代)
        # fetchall=False: return成功执行数(1)
        if dictionary is None:
            dictionary = self.dictionary
        cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        if args and empty_string_to_none:
            args = tuple(each if each != '' else None for each in args)
        try_count_connect = 0
        while True:
            try:
                cursor.callproc(name, args)
                if commit and not self._autocommit:
                    self.connection.commit()
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
                # 执行失败便不会产生效果，没有需要一并回滚的
                # self.connection.rollback()
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
