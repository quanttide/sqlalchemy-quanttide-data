# -*- coding: utf-8 -*-
"""
数据存储模块
"""

from typing import Any, Optional, Union, Collection


class WriterMixin(object):
    """
    数据保存Mixin类
    """
    def save_data(self, args: Any, table: Optional[str] = None, statement: Optional[str] = None,
                  extra: Optional[str] = None, not_one_by_one: Optional[bool] = False,
                  keys: Union[str, Collection[str], None] = None, commit: Optional[bool] = None,
                  escape_auto_format: Optional[bool] = None, escape_formatter: Optional[str] = None,
                  empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                  time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None
                  ) -> Union[int, tuple, list]:
        """
        data_list 支持单条记录: list/tuple/dict，或多条记录: list/tuple/set[list/tuple/dict]
        首条记录需为dict(one_by_one=True时所有记录均需为dict)，或者含除自增字段外所有字段并按顺序排好各字段值，或者自行传入keys
        默认not_one_by_one=False: 为了部分记录无法插入时能够单独跳过这些记录(有log)
        fetchall=False: return成功执行语句数(executemany模式即not_one_by_one=True时按数据条数)
        """
        # 处理参数
        if not args and args not in (0, ''):
            return 0
        # 生成SQL语句
        query = '{} {}{{}} VALUES({{}}){}'.format(
            self.statement_save_data if statement is None else statement, self.table if table is None else table,
            ' {}'.format(extra) if extra is not None else '')
        # 执行SQL查询
        return self.query(query, args, False, False, not_one_by_one, True, keys, commit, escape_auto_format,
                          escape_formatter, empty_string_to_none, False, (), try_times_connect, time_sleep_connect,
                          raise_error)
