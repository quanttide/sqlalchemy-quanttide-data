# -*- coding: utf-8 -*-
"""
执行器模块
"""


class ExecutorMixin(object):
    def try_execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                    many: bool = False, commit: Optional[bool] = None,
                    try_times_connect: Union[int, float, None] = None,
                    time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None,
                    cursor: Any = None) -> Union[int, tuple, list]:
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        if try_times_connect is None:
            try_times_connect = self.try_times_connect
        if time_sleep_connect is None:
            time_sleep_connect = self.time_sleep_connect
        if raise_error is None:
            raise_error = self.raise_error
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        try_count_connect = 0
        while True:
            try:
                result = self.execute(query, args, fetchall, dictionary, many, commit, cursor)
                if ori_cursor is None:
                    cursor.close()
                return result
            except (self.lib.InterfaceError, self.lib.OperationalError) as e:
                try_count_connect += 1
                if try_times_connect and try_count_connect >= try_times_connect:
                    if self.log:
                        self.logger.error('{}(max retry({})): {}  {}'.format(
                            str(type(e))[8:-2], try_count_connect, e, self._query_log_text(query, args)), exc_info=True)
                    if raise_error:
                        if ori_cursor is None:
                            cursor.close()
                        raise e
                    break
                if self.log:
                    self.logger.error('{}(retry({}), sleep {}): {}  {}'.format(
                        str(type(e))[8:-2], try_count_connect, time_sleep_connect, e,
                        self._query_log_text(query, args)), exc_info=True)
                if time_sleep_connect:
                    time.sleep(time_sleep_connect)
            except Exception as e:
                self.rollback()
                if self.log:
                    self.logger.error('{}: {}  {}'.format(
                        str(type(e))[8:-2], e, self._query_log_text(query, args)), exc_info=True)
                if raise_error:
                    if ori_cursor is None:
                        cursor.close()
                    raise e
                break
        if fetchall:
            return ()
        return 0

    def execute(self, query: str, args: Any = None, fetchall: bool = True, dictionary: Optional[bool] = None,
                many: bool = False, commit: Optional[bool] = None, cursor: Any = None) -> Union[int, tuple, list]:
        # fetchall=False: return成功执行语句数(executemany模式按数据条数)
        ori_cursor = cursor
        if cursor is None:
            cursor = self._before_query_and_get_cursor(fetchall, dictionary)
        if not many:
            cursor.execute(query, args)
        else:  # executemany: 一句插入多条记录，当语句超出1024000字符时拆分成多个语句；传单条记录需用列表包起来
            cursor.executemany(query, args)
        if commit and not self._autocommit:
            self.commit()
        result = cursor.fetchall() if fetchall else len(args) if many and hasattr(args, '__len__') else 1
        if ori_cursor is None:
            cursor.close()
        return result
