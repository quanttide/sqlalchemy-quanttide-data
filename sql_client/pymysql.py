# -*- coding: utf-8 -*-

import pymysql

import sql_utils.base


class SqlUtil(sql_utils.base.SqlUtil):
    lib = pymysql

    def __init__(self, host, port=3306, user=None, password=None, database=None, charset='utf8mb4', autocommit=True,
                 connect_now=True, log=True, table=None, statement_save_data='REPLACE', dictionary=False,
                 escape_auto_format=True, escape_formatter='`{}`', empty_string_to_none=True, keep_args_as_dict=False,
                 try_times_connect=3, time_sleep_connect=3, raise_error=False):
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         keep_args_as_dict, try_times_connect, time_sleep_connect, raise_error)