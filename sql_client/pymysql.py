# -*- coding: utf-8 -*-

from typing import Union, Optional

import pymysql

from .base import SqlClient as BaseSqlClient, Paramstyle, NOTSET, Notset


class SqlClient(BaseSqlClient):
    lib = pymysql

    def __init__(self, host: Optional[str] = None, port: Union[int, str, None] = 3306, user: Optional[str] = None,
                 password: Optional[str] = None, database: Optional[str] = None, charset: Optional[str] = 'utf8mb4',
                 autocommit: bool = True, connect_now: bool = True, log: bool = True, table: Optional[str] = None,
                 statement_save_data: str = 'REPLACE', dictionary: bool = False, escape_auto_format: bool = True,
                 escape_formatter: str = '`{}`', empty_string_to_none: bool = True, args_to_dict: Optional[bool] = None,
                 to_paramstyle: Optional[Paramstyle] = Paramstyle.format, try_reconnect: bool = True,
                 try_times_connect: Union[int, float] = 3, time_sleep_connect: Union[int, float] = 3,
                 raise_error: bool = False, exc_info: Optional[bool] = None):
        super().__init__(host, port, user, password, database, charset, autocommit, connect_now, log, table,
                         statement_save_data, dictionary, escape_auto_format, escape_formatter, empty_string_to_none,
                         args_to_dict, to_paramstyle, try_reconnect, try_times_connect, time_sleep_connect, raise_error,
                         exc_info)

    def reconnect(self, exc_info: Union[bool, Notset, None] = NOTSET) -> None:
        if self.connection is not None:
            try:
                self.connection.connect()
                self.connected = True
                return
            except Exception as e:
                if self.log:
                    self.logger.error('{}: {}  (in reconnect)'.format(str(type(e))[8:-2], e),
                                      exc_info=True if exc_info is None else exc_info)
        self.connect()
