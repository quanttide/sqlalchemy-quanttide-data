# -*- coding: utf-8 -*-

import MySQLdb

from sql_client.base import SqlClient as BaseSqlClient


class SqlClient(BaseSqlClient):
    """
    Linux安装命令:
    sudo apt-get install libmysqlclient-dev
    pip3 install mysqlclient
    """
    lib = MySQLdb

    def __init__(self, host=None, port=3306, user=None, password=None, database=None, charset='utf8mb4',
                 autocommit=True, connect_now=True, log=True, table=None, statement_save_data='REPLACE',
                 dictionary=False, escape_auto_format=True, escape_formatter='`{}`', empty_string_to_none=True,
                 keep_args_as_dict=True, transform_formatter=True, try_times_connect=3, time_sleep_connect=3,
                 raise_error=False):
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         keep_args_as_dict, transform_formatter, try_times_connect, time_sleep_connect, raise_error)
