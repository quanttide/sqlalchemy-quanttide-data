import cx_Oracle
import time

import base_sql_util


class SqlUtil(base_sql_util.SqlUtil):
    def __init__(self, host, port=1521, user=None, password=None, database=None, charset='utf8', autocommit=True,
                 connect_now=True, dictionary=True, log=True):
        self.lib = cx_Oracle
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, dictionary, log)

    def query(self, query, args=None, fetchall=True, dictionary=None, many=True, commit=True, auto_format=False,
              escape_auto_format=False, empty_string_to_none=True, keep_args_as_dict=False, try_times_connect=3,
              time_sleep_connect=3, raise_error=False):
        # oracle如果用双引号escape字段则区分大小写，故默认不escape
        return super().query(query, args, fetchall, dictionary, many, commit, auto_format, escape_auto_format,
                             empty_string_to_none, keep_args_as_dict, try_times_connect, time_sleep_connect,
                             raise_error)

    def save_data(self, data_list=None, table=None, statement='INSERT INTO', extra=None, many=False, auto_format=True,
                  key=None, escape_auto_format=True, empty_string_to_none=True, keep_args_as_dict=False,
                  try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # oracle无replace语句；insert必须带into
        return super().save_data(data_list, table, statement, extra, many, auto_format, key, escape_auto_format,
                                 empty_string_to_none, keep_args_as_dict, try_times_connect, time_sleep_connect,
                                 raise_error)

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        if hasattr(self, 'connection') and value != self._autocommit:
            self.connection.autocommit = value
        self._autocommit = value

    def connect(self):
        self.connection = self.lib.connect(user=self.user, password=self.password, dsn='{}:{}/{}'.format(
            self.host, self.port, self.database), encoding=self.charset)
        self.connection.autocommit = self._autocommit

    def execute(self, query, values=None, fetchall=True, dictionary=None, many=False, commit=True, cursor=None):
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, self.dictionary if dictionary is None else dictionary)
        if not many:
            cursor.execute(query, () if values is None else values)
        else:  # executemany: 一句插入多条记录，当语句超出1024000字符时拆分成多个语句；传单条记录需用列表包起来
            cursor.executemany(query, values)
        if commit and not self._autocommit:
            self.commit()
        result = cursor.fetchall() if fetchall else 1 if not many else len(values)
        if ori_cursor is None:
            cursor.close()
        return result

    def format(self, query, args, raise_error=True):
        # cx_Oracle.Connection没有literal和escape，暂不借鉴mysql实现
        try:
            if args is None:
                return query
            return query % args if '%' in query else query.format(args)
        except Exception as e:
            if raise_error:
                raise e
            return
    @staticmethod
    def _auto_format_query(query, arg, escape_auto_format):
        return query.format('({})'.format(','.join(('"{}"'.format(key) for key in arg) if escape_auto_format else
                                                   map(str, arg))) if isinstance(arg, dict) else '',
                            ','.join(':{}'.format(i) for i in range(1, len(arg) + 1)))  # oracle不使用``而使用""

    def _before_query_and_get_cursor(self, fetchall, dictionary):
        self.set_connection()
        if fetchall and (self.dictionary if dictionary is None else dictionary):
            cursor = self.connection.cursor()
            cursor.rowfactory = lambda *args: dict(zip((col[0] for col in cursor.description), args))
            return cursor
        return self.connection.cursor()

    def _query_log_text(self, query, values):
        return 'query: {}'.format(self.try_format(query, values))


    def call_proc(self, name, args=(), kwargs=None, fetchall=True, dictionary=None, commit=True,
                  empty_string_to_none=True, try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # 增加kwargs
        # 执行存储过程
        # name: 存储过程名
        # args: 存储过程参数(不能为None，要可迭代)
        # fetchall=False: return成功执行数(1)
        if kwargs is None:
            kwargs = {}
        if dictionary is None:
            dictionary = self.dictionary
        cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        if args and empty_string_to_none:
            args = tuple(each if each != '' else None for each in args)
        try_count_connect = 0
        while True:
            try:
                cursor.callproc(name, args, kwargs)
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

    def call_func(self, name, return_type, args=(), kwargs=None, fetchall=True, dictionary=None, commit=True,
                  empty_string_to_none=True, try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # 执行函数
        # name: 函数名
        # return_type: 返回值的类型(必填)，参见https://cx-oracle.readthedocs.io/en/latest/user_guide/plsql_execution.html#plsqlfunc, https://cx-oracle.readthedocs.io/en/latest/api_manual/cursor.html#Cursor.callfunc
        # args: 函数参数(不能为None，要可迭代)
        # fetchall=False: return成功执行数(1)
        if kwargs is None:
            kwargs = {}
        if dictionary is None:
            dictionary = self.dictionary
        cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        if args and empty_string_to_none:
            args = tuple(each if each != '' else None for each in args)
        try_count_connect = 0
        while True:
            try:
                cursor.callfunc(name, return_type, args, kwargs)
                if commit and not self._autocommit:
                    self.connection.commit()
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
                # 执行失败便不会产生效果，没有需要一并回滚的
                # self.connection.rollback()
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
