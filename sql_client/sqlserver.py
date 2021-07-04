# -*- coding: utf-8 -*-

import pymssql

import sql_utils.base


class SqlUtil(sql_utils.base.SqlUtil):
    lib = pymssql

    def __init__(self, host, port=1433, user=None, password=None, database=None, charset='utf8', autocommit=True,
                 connect_now=True, log=True, table=None, statement_save_data='INSERT', dictionary=False,
                 escape_auto_format=True, escape_formatter='[{}]', empty_string_to_none=True, keep_args_as_dict=False,
                 try_times_connect=3, time_sleep_connect=3, raise_error=False):
        # sqlserver无replace语句
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         keep_args_as_dict, try_times_connect, time_sleep_connect, raise_error)

    def begin(self):
        self.temp_autocommit = self._autocommit
        self.autocommit = False
        self.set_connection()

    def connect(self):
        self.connection = self.lib.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.database, charset=self.charset, autocommit=self._autocommit,
                                           as_dict=self.dictionary)

    def ping(self):
        # pymssql.Connection没有ping
        self.set_connection()

    def format(self, query, args, raise_error=True):
        # pymssql.Connection没有literal和escape，暂不借鉴mysql实现
        try:
            if args is None:
                return query
            return query % args if '%' in query else query.format(args)
        except Exception as e:
            if raise_error:
                raise e
            return

    def _before_query_and_get_cursor(self, fetchall=True, dictionary=None):
        if fetchall and dictionary is not None and dictionary != self.dictionary:
            if hasattr(self, 'connection'):
                self.connection.as_dict = dictionary
            self.dictionary = dictionary
        self.set_connection()
        return self.connection.cursor()
