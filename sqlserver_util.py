import pymssql

import base_sql_util


class SqlUtil(base_sql_util.SqlUtil):
    def __init__(self, host, port=1433, user=None, password=None, database=None, charset='utf8', autocommit=True,
                 connect_now=True, dictionary=True, log=True):
        self.lib = pymssql
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, dictionary, log)

    def connect(self):
        self.connection = self.lib.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.database, charset=self.charset, autocommit=self._autocommit,
                                           as_dict=self.dictionary)

    def begin(self):
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()

    def ping(self):
        # pymssql.Connection没有ping
        pass

    def format(self, query, args, raise_error=True):
        # pymssql.Connection没有literal和escape，暂不借鉴mysql实现
        try:
            if args is None:
                return query
            return query % args
        except Exception as e:
            if raise_error:
                raise e
            return

    def _query_log_text(self, query, values):
        return 'query: {}'.format(self.try_format(query, values))

    def _before_query_and_get_cursor(self, fetchall, dictionary):
        if fetchall and dictionary != self.dictionary:
            if hasattr(self, 'connection'):
                self.connection.as_dict = dictionary
            self.dictionary = dictionary
        self.set_connection()
        return self.connection.cursor()

    @staticmethod
    def _auto_format_query(query, arg, escape_auto_format):
        return query.format('({})'.format(','.join(('[{}]'.format(key) for key in arg) if escape_auto_format else map(
            str, arg))) if isinstance(arg, dict) else '', ','.join(('%s',) * len(arg)))  # sqlserver不使用``而使用[]

    def save_data(self, data_list=None, table=None, statement='INSERT', extra=None, many=False, auto_format=True,
                  key=None, escape_auto_format=True, empty_string_to_none=True, keep_args_as_dict=False,
                  try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # sqlserver无replace语句
        return super().save_data(data_list, table, statement, extra, many, auto_format, key, escape_auto_format,
                                 empty_string_to_none, keep_args_as_dict, try_times_connect, time_sleep_connect,
                                 raise_error)
