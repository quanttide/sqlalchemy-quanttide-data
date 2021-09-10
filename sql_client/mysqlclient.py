# -*- coding: utf-8 -*-

from typing import Union, Optional

import MySQLdb

from .base import SqlClient as BaseSqlClient, Paramstyle


class SqlClient(BaseSqlClient):
    lib = MySQLdb

    def __init__(self, host: Optional[str] = None, port: Union[int, str, None] = 3306, user: Optional[str] = None,
                 password: Optional[str] = None, database: Optional[str] = None, charset: Optional[str] = 'utf8mb4',
                 autocommit: bool = True, connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: str = 'REPLACE', dictionary: bool = False, escape_auto_format: bool = True,
                 escape_formatter: str = '`{}`', empty_string_to_none: bool = True, args_to_dict: Optional[bool] = None,
                 to_paramstyle: Optional[Paramstyle] = Paramstyle.format, try_times_connect: Union[int, float] = 3,
                 time_sleep_connect: Union[int, float] = 3, raise_error: bool = False):
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         args_to_dict, to_paramstyle, try_times_connect, time_sleep_connect, raise_error)
