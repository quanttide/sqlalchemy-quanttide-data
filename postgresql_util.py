import psycopg2
import psycopg2.extras
import time

import base_sql_util


class SqlUtil(base_sql_util.SqlUtil):
    def __init__(self, host, port=5432, user=None, password=None, database=None, charset=None, autocommit=True,
                 connect_now=True, dictionary=True, log=True):
        self.lib = psycopg2
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, dictionary, log)

    def connect(self):
        self.connection = self.lib.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.database)
        self.connection.autocommit = self._autocommit

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        if hasattr(self, 'connection') and value != self._autocommit:
            self.connection.autocommit = value
        self._autocommit = value

    def begin(self):
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()

    def ping(self):
        # psycopg2.connection没有ping
        pass

    def format(self, query, args, raise_error=True, cursor=None):
        # psycopg2.connection没有literal和escape，但psycopg2.cursor有mogrify
        try:
            if args is None:
                return query
            ori_cursor = cursor
            if cursor is None:
                cursor = self.connection.cursor()
            if isinstance(args, dict):
                new_args = dict((key, cursor.mogrify(item)) for key, item in args.items())
                formatted_query = query % new_args if '%' in query else query.format(**new_args)
            else:
                new_args = tuple(map(cursor.mogrify, args))
                formatted_query = query % new_args if '%' in query else query.format(*new_args)
            if ori_cursor is None:
                cursor.close()
            return formatted_query
        except Exception as e:
            if raise_error:
                raise e
            return

    def try_format(self, query, args, cursor=None):
        try:
            return self.format(query, args, True, cursor)
        except Exception as e:
            return '{}: {}: {}'.format(query, str(type(e))[8:-2], e)

    def _query_log_text(self, query, values, cursor=None):
        return 'formatted_query: {}'.format(self.try_format(query, values, cursor))

    def _before_query_and_get_cursor(self, fetchall, dictionary):
        if dictionary and fetchall:
            cursor_class = self.lib.extras.DictCursor
        else:
            cursor_class = None
        self.set_connection()
        return self.connection.cursor(cursor_factory=cursor_class)

    def try_execute(self, query, values=None, args=None, cursor=None, fetchall=True, dictionary=None, many=False,
                    commit=True, try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # psycopg2.connection没有literal和escape，但psycopg2.cursor有mogrify
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        # values可以为None
        if dictionary is None:
            dictionary = self.dictionary
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
                            str(type(e))[8:-2], try_count_connect, e, args,
                            self._query_log_text(query, values, cursor)), exc_info=True)
                    if raise_error:
                        if ori_cursor is None:
                            cursor.close()
                        raise e
                    break
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  args: {}  {}'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e, args,
                        self._query_log_text(query, values, cursor)), exc_info=True)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                self.rollback()
                if self.log:
                    self.logger.error('{}: {}  args: {}  {}'.format(
                        str(type(e))[8:-2], e, args, self._query_log_text(query, values, cursor)), exc_info=True)
                if raise_error:
                    if ori_cursor is None:
                        cursor.close()
                    raise e
                break
        if fetchall:
            return ()
        return 0

    @staticmethod
    def _auto_format_query(query, arg, escape_auto_format):
        return query.format('({})'.format(','.join(('"{}"'.format(key) for key in arg) if escape_auto_format else map(
            str, arg))) if isinstance(arg, dict) else '', ','.join(('%s',) * len(arg)))  # postgresql不使用``而使用""

    def query(self, query, args=None, fetchall=True, dictionary=None, many=True, commit=True, auto_format=False,
              escape_auto_format=False, empty_string_to_none=True, keep_args_as_dict=False, try_times_connect=3,
              time_sleep_connect=3, raise_error=False):
        # postgresql如果用双引号escape字段则区分大小写，故默认不escape
        return super().query(query, args, fetchall, dictionary, many, commit, auto_format, escape_auto_format,
                             empty_string_to_none, keep_args_as_dict, try_times_connect, time_sleep_connect,
                             raise_error)

    def save_data(self, data_list=None, table=None, statement='INSERT INTO', extra=None, many=False, auto_format=True,
                  key=None, escape_auto_format=True, empty_string_to_none=True, keep_args_as_dict=False,
                  try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # postgresql无replace语句；insert必须带into
        return super().save_data(data_list, table, statement, extra, many, auto_format, key, escape_auto_format,
                                 empty_string_to_none, keep_args_as_dict, try_times_connect, time_sleep_connect,
                                 raise_error)
