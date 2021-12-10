# -*- coding: utf-8 -*-
"""
数据库连接模块
"""


class ConnectorMixin(object):
    def connect(self) -> None:
        self.connection = self.lib.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                           database=self.database, charset=self.charset, autocommit=self._autocommit)

    def set_connection(self) -> None:
        if not hasattr(self, 'connection'):
            self.try_connect()

    def try_connect(self, try_times_connect: Union[int, float, None] = None,
                    time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None) -> None:
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

    def rollback(self) -> None:
        if hasattr(self, 'connection'):
            self.connection.rollback()
        if self.temp_autocommit is not None:
            self.autocommit = self.temp_autocommit
            self.temp_autocommit = None

    def ping(self) -> None:
        try:
            self.connection.ping()
        except (self.lib.InterfaceError, self.lib.OperationalError):
            # MySQLdb._exceptions.OperationalError: (2013, 'Lost connection to MySQL server during query')
            # MySQLdb._exceptions.OperationalError: (2006, 'MySQL server has gone away')
            # _mysql_exceptions.InterfaceError: (0, '')
            self.close()
            self.try_connect()
        except AttributeError:
            # AttributeError: 'SqlClient' object has no attribute 'connection'
            # AttributeError: 'NoneType' object has no attribute 'ping'
            self.try_connect()
