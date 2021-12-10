# -*- coding: utf-8 -*-
"""
任务调度模块
"""

from typing import Optional, Union, Iterable


class DispatcherMixin(object):
    def select_to_try(self, table: Optional[str] = None, num: Union[int, str, None] = 1,
                      key_fields: Union[str, Iterable[str]] = 'id', extra_fields: Union[str, Iterable[str], None] = '',
                      tried_field: Optional[str] = None, tried: Union[int, str, tuple, None] = 'between',
                      tried_min: Union[int, str, None] = 1, tried_max: Union[int, str, None] = 5,
                      tried_after: Union[int, str, tuple, None] = '-', finished_field: Optional[str] = None,
                      finished: Union[int, str, None] = 0, next_time_field: Optional[str] = None,
                      next_time: Union[int, float, str, tuple, None] = None,
                      next_time_after: Union[int, float, str, tuple, None] = (), lock: bool = True,
                      dictionary: Optional[bool] = None, autocommit_after: Optional[bool] = None,
                      select_where: Optional[str] = None, select_extra: str = '', set_extra: Optional[str] = '',
                      update_set: Optional[str] = None, update_where: Optional[str] = None, update_extra: str = '',
                      empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                      time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None
                      ) -> Union[int, tuple, list]:
        """
        :param key_fields: update一句where部分使用
        :param extra_fields: 不在update一句使用, return结果包含key_fields和extra_fields
        :param tried_field, finished_field, next_time_field字段传入与否分别决定相关逻辑启用与否, 默认值None表示不启用
        :param tried: 默认值'between'表示取tried_min<=tried_field<=tried_max, 也可传入'>=0'等(传入空元组()表示不限制),
               如需多个条件, 可传入空元组()并往select_extra传入例如' and (<tried_field> is null or <tried_field> <= <time>)'
        :param tried_after: 默认值'-'表示取tried_field当前值的相反数, 也可传入'+1'等
                     (传入空元组()表示不修改, 传入None则设为null, 传入''则根据empty_string_to_none决定是否转为null)
        :param next_time: 默认值None表示取next_time_field<=当前timestamp整数部分(注意取不到为null的记录), 传入空元组()表示不限制, 如需多个条件,
                   可传入空元组()并往select_extra传入例如' and (<next_time_field> is null or <next_time_field> <= <time>)'
        :param next_time_after: 默认值空元组()表示不修改, 传入None则设为null, 传入''则根据empty_string_to_none决定是否转为null
        :param select_where: 不为None则替换select一句的where部分(为''时删除where)
        :param update_set: 不为None则替换update一句的set部分
        :param update_where: 不为None则替换update一句的where部分
        """
        if table is None:
            table = self.table
        if isinstance(key_fields, str):
            key_fields_list = [key.strip() for key in key_fields.split(',')]
        else:
            key_fields_list = key_fields
            key_fields = ','.join(key_fields)
        if extra_fields is not None and not isinstance(extra_fields, str):
            extra_fields = ','.join(extra_fields)
        args = []
        if select_where is None:
            if not tried_field or tried == ():
                select_tried = ''
            elif tried is None or tried == 'null':
                select_tried = tried_field + ' is null'
            elif tried == 'between':
                select_tried = '{} between {} and {}'.format(tried_field, tried_min, tried_max)
            elif isinstance(tried, str) and tried.lstrip().startswith(('>', '=', '<', 'between')):
                if tried.startswith('between'):
                    select_tried = tried_field + ' ' + tried
                else:
                    select_tried = tried_field + tried
            elif isinstance(tried, int):
                select_tried = '{}={}'.format(tried_field, tried)
            else:
                select_tried = tried_field + '=%s'
                args.append(tried)
            if not finished_field:
                select_finished = ''
            elif finished is None or finished == 'null':
                select_finished = finished_field + ' is null'
            elif isinstance(finished, str) and finished.lstrip().startswith(('>', '=', '<', 'between')):
                if finished.startswith('between'):
                    select_finished = finished_field + ' ' + finished
                else:
                    select_finished = finished_field + finished
            elif isinstance(finished, int):
                select_finished = '{}={}'.format(finished_field, finished)
            else:
                select_finished = finished_field + '=%s'
                args.append(finished)
            if not next_time_field or next_time == ():
                select_next_time = ''
            elif next_time is None:
                select_next_time = '{}<={}'.format(next_time_field, int(time.time()))
            elif next_time == 'null':
                select_next_time = next_time_field + ' is null'
            elif isinstance(next_time, str) and next_time.lstrip().startswith(('>', '=', '<', 'between')):
                if next_time.startswith('between'):
                    select_next_time = next_time_field + ' ' + next_time
                else:
                    select_next_time = next_time_field + next_time
            elif isinstance(next_time, (int, float)):
                select_next_time = '{}<={}'.format(next_time_field,
                                                   int(time.time()) + next_time if next_time < 10 ** 9 else next_time)
            else:
                select_next_time = next_time_field + '<=%s'
                args.append(next_time)
            select_where = ' where ' + ' and '.join(filter(None, (select_tried, select_finished, select_next_time)))
        elif select_where:
            if select_where.startswith('where'):
                select_where = ' ' + select_where
            elif not select_where.startswith(' where'):
                select_where = ' where ' + select_where.lstrip(' ')
        query = 'select {}{} from {}{}{}{}{}'.format(key_fields, ',' + extra_fields if extra_fields else '', table,
                                                     select_where, select_extra, ' limit {}'.format(num) if num else '',
                                                     ' for update' if lock else '')
        self.begin()
        result = self.query(query, args, fetchall=True, dictionary=dictionary, commit=False,
                            empty_string_to_none=empty_string_to_none, try_times_connect=try_times_connect,
                            time_sleep_connect=time_sleep_connect, raise_error=raise_error)
        if not result:
            self.commit()
            if autocommit_after is not None:
                self.autocommit = autocommit_after
            return result
        args = []
        if not tried_field or tried_after == ():
            update_tried = ''
        elif tried_after == '-':
            update_tried = '{0}=-{0}'.format(tried_field)
        elif tried_after == '+1':
            update_tried = '{0}={0}+1'.format(tried_field)
        elif tried_after == '-+1':
            update_tried = '{0}=-{0}+1'.format(tried_field)
        elif isinstance(tried_after, str) and tried_after.lstrip().startswith('='):
            update_tried = tried_field + tried_after
        elif isinstance(tried_after, int):
            update_tried = '{}={}'.format(tried_field, tried_after)
        else:
            update_tried = tried_field + '=%s'
            args.append(tried_after)
        if not next_time_field or next_time_after == ():
            update_next_time = ''
        elif isinstance(next_time_after, (int, float)):
            update_next_time = '{}={}'.format(next_time_field,
                                              int(time.time()) + next_time_after if next_time_after < 10 ** 9 else
                                              next_time_after)
        elif isinstance(next_time_after, str) and next_time_after.lstrip().startswith('='):
            update_next_time = next_time_field + next_time_after
        else:
            update_next_time = next_time_field + '=%s'
            args.append(next_time_after)
        if update_where is None:
            update_where = ' or '.join((' and '.join(map('{}=%s'.format, key_fields_list)),) * len(result))
            if dictionary:
                args.extend(row[key] for row in result for key in key_fields_list)
            else:
                args.extend(row[i] for row in result for i in range(len(key_fields_list)))
        elif update_where.startswith('where'):
            update_where = update_where[5:].lstrip(' ')
        elif update_where.startswith(' where'):
            update_where = update_where[6:].lstrip(' ')
        query = 'update {} set {} where {}{}'.format(table, ','.join(filter(None, (update_tried, update_next_time)))
                                                            + set_extra if update_set is None else update_set,
                                                     update_where, update_extra)
        is_success = self.query(query, args, fetchall=False, commit=False, empty_string_to_none=empty_string_to_none,
                                try_times_connect=try_times_connect, time_sleep_connect=time_sleep_connect,
                                raise_error=raise_error)
        if is_success:
            self.commit()
        else:
            result = ()
            self.rollback()
        if autocommit_after is not None:
            self.autocommit = autocommit_after
        return result

    def end_try(self, result: Optional[Iterable], table: Optional[str] = None,
                key_fields: Union[str, Iterable[str], None] = None, tried_field: Optional[str] = None,
                tried: Union[int, str, None] = 0, finished_field: Optional[str] = None,
                finished: Union[int, str, None] = 1, next_time_field: Optional[str] = None,
                next_time: Union[int, float, str, None] = '=0', commit: bool = True, set_extra: Optional[str] = '',
                update_set: Optional[str] = None, update_where: Optional[str] = None, update_extra: str = '',
                empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None) -> int:
        # key_fields为''或None时，result需为dict或list[dict]，key_fields取result的keys
        # tried_field, finished_field, next_time_field字段传入与否分别决定相关逻辑启用与否, 默认值None表示不启用
        # update_where: 不为None则替换update一句的where部分
        result = self.standardize_args(result, True, False, None, False)
        if not result:
            return 0
        if table is None:
            table = self.table
        if not key_fields:
            key_fields = tuple(result[0].keys())
        elif isinstance(key_fields, str):
            key_fields = [key.strip() for key in key_fields.split(',')]
        args = []
        if not tried_field:
            update_tried = ''
        elif tried == '-+1':
            update_tried = '{0}=-{0}+1'.format(tried_field)
        elif tried == '-':
            update_tried = '{0}=-{0}'.format(tried_field)
        elif tried == '+1':
            update_tried = '{0}={0}+1'.format(tried_field)
        elif isinstance(tried, str) and tried.lstrip().startswith('='):
            update_tried = tried_field + tried
        elif isinstance(tried, int):
            update_tried = '{}={}'.format(tried_field, tried)
        else:
            update_tried = tried_field + '=%s'
            args.append(tried)
        if not finished_field:
            update_finished = ''
        elif isinstance(finished, str) and finished.lstrip().startswith('='):
            update_finished = finished_field + finished
        elif isinstance(finished, int):
            update_finished = '{}={}'.format(finished_field, finished)
        else:
            update_finished = finished_field + '=%s'
            args.append(finished)
        if not next_time_field:
            update_next_time = ''
        elif isinstance(next_time, (int, float)):
            update_next_time = '{}={}'.format(next_time_field,
                                              int(time.time()) + next_time if next_time < 10 ** 9 else next_time)
        elif isinstance(next_time, str) and next_time.lstrip().startswith('='):
            update_next_time = next_time_field + next_time
        else:
            update_next_time = next_time_field + '=%s'
            args.append(next_time)
        if update_where is None:
            update_where = ' or '.join((' and '.join(map('{}=%s'.format, key_fields)),) * len(result))
            if isinstance(result[0], dict):
                args.extend(row[key] for row in result for key in key_fields)
            else:
                args.extend(row[i] for row in result for i in range(len(key_fields)))
        elif update_where.startswith('where'):
            update_where = update_where[5:].lstrip(' ')
        elif update_where.startswith(' where'):
            update_where = update_where[6:].lstrip(' ')
        query = 'update {} set {} where {}{}'.format(table, ','.join(filter(None, (
            update_tried, update_finished, update_next_time))) + set_extra if update_set is None else update_set,
                                                     update_where, update_extra)
        return self.query(query, args, fetchall=False, commit=commit, empty_string_to_none=empty_string_to_none,
                          try_times_connect=try_times_connect, time_sleep_connect=time_sleep_connect,
                          raise_error=raise_error)

    def fail_try(self, result: Optional[Iterable], table: Optional[str] = None,
                 key_fields: Union[str, Iterable[str], None] = None, tried_field: Optional[str] = None,
                 tried: Union[int, str, None] = '-+1', finished_field: Optional[str] = None,
                 finished: Union[int, str, None] = 0, next_time_field: Optional[str] = None,
                 next_time: Union[int, float, str, None] = 300, commit: bool = True, set_extra: Optional[str] = '',
                 update_set: Optional[str] = None, update_where: Optional[str] = None, update_extra: str = '',
                 empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                 time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None) -> int:
        # 复用end_try, 仅改变tried, finished, next_time参数默认值
        return self.end_try(result, table, key_fields, tried_field, tried, finished_field, finished, next_time_field,
                            next_time, commit, set_extra, update_set, update_where, update_extra, empty_string_to_none,
                            try_times_connect, time_sleep_connect, raise_error)

    def cancel_try(self, result: Optional[Iterable], table: Optional[str] = None,
                   key_fields: Union[str, Iterable[str], None] = None, tried_field: Optional[str] = None,
                   tried: Union[int, str, None] = '-', finished_field: Optional[str] = None,
                   finished: Union[int, str, None] = 0, next_time_field: Optional[str] = None,
                   next_time: Union[int, float, str, None] = '=0', commit: bool = True, set_extra: Optional[str] = '',
                   update_set: Optional[str] = None, update_where: Optional[str] = None, update_extra: str = '',
                   empty_string_to_none: Optional[bool] = None, try_times_connect: Union[int, float, None] = None,
                   time_sleep_connect: Union[int, float, None] = None, raise_error: Optional[bool] = None) -> int:
        # 取消尝试, 恢复select_to_try以前的原状
        # 复用end_try, 仅改变tried参数默认值
        # finished_field, next_time_field建议不填写, 以保持原状
        return self.end_try(result, table, key_fields, tried_field, tried, finished_field, finished, next_time_field,
                            next_time, commit, set_extra, update_set, update_where, update_extra, empty_string_to_none,
                            try_times_connect, time_sleep_connect, raise_error)

