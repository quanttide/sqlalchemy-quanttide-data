# -*- coding: utf-8 -*-
"""
格式转换模块
"""

from typing import Any, Optional, Union, Iterable, Tuple, Sequence


class InputFormatterMixin(object):
    """
    输入值处理Mixin类
    """
    def standardize_args(self, args: Any, to_multiple: Optional[bool] = None,
                         empty_string_to_none: Optional[bool] = None, args_to_dict: Union[bool, None, tuple] = (),
                         get_info: bool = False, keys: Optional[Iterable[str]] = None,
                         nums: Optional[Iterable[int]] = None
                         ) -> Union[Sequence, dict, None, Tuple[Union[Sequence, dict], bool, bool]]:
        """
        get_info=True: 返回值除了标准化的变量以外还有: 是否multiple, 字典key是否为生成的
        args_to_dict=None: 不做dict和list之间转换; args_to_dict=False: dict强制转为list; args_to_dict=(): 读取默认配置
        args_to_dict=False且args为dict形式时，keys需不为None(query方法已预先处理)
        nums: from_paramstyle=Paramstyle.numeric且to_paramstyle为Paramstyle.format或Paramstyle.qmark且通配符乱序时，传入通配符数字列表
        """
        if args is None:
            return None if not get_info else (None, False, False)
        if not args and args not in (0, ''):
            return () if not get_info else ((), False, False)
        if not hasattr(args, '__getitem__'):
            if hasattr(args, '__iter__'):  # set, Generator, range
                args = tuple(args)
                if not args:
                    return args if not get_info else (args, False, False)
            else:  # int, etc.
                args = (args,)
        elif isinstance(args, str):
            args = (args,)
        # else: dict, list, tuple, dataset/row, recordcollection/record
        if to_multiple is None:  # 检测是否multiple
            to_multiple = not isinstance(args, dict) and not isinstance(args[0], str) and (
                    hasattr(args[0], '__getitem__') or hasattr(args[0], '__iter__'))
        if isinstance(args_to_dict, tuple):
            args_to_dict = self.args_to_dict
        if empty_string_to_none is None:
            empty_string_to_none = self.empty_string_to_none
        is_key_generated = False
        if not to_multiple:
            if isinstance(args, dict):
                if args_to_dict is False:
                    args = tuple(args[key] if args[key] != '' else None for key in keys
                                 ) if empty_string_to_none else tuple(map(args.__getitem__, keys))
                elif empty_string_to_none:
                    args = {key: value if value != '' else None for key, value in args.items()}
            else:
                if args_to_dict:
                    if keys is None:
                        keys = tuple(map(str, range(1, len(args) + 1)))
                        is_key_generated = True
                    args = {key: value if value != '' else None for key, value in zip(keys, args)
                            } if empty_string_to_none else dict(zip(keys, args))
                elif nums is not None:
                    args = tuple(args[num] if args[num] != '' else None for num in nums
                                 ) if empty_string_to_none else tuple(map(args.__getitem__, nums))
                elif empty_string_to_none:
                    args = tuple(each if each != '' else None for each in args)
        else:
            if isinstance(args, dict):
                if args_to_dict is False:
                    args = (tuple(args[key] if args[key] != '' else None for key in keys),
                            ) if empty_string_to_none else (tuple(map(args.__getitem__, keys)),)
                else:
                    args = ({key: value if value != '' else None for key, value in args.items()},
                            ) if empty_string_to_none else (args,)
            elif isinstance(args[0], dict):
                if args_to_dict is False:
                    args = tuple(tuple(each[key] if each[key] != '' else None for key in keys) for each in args
                                 ) if empty_string_to_none else tuple(
                        tuple(map(each.__getitem__, keys)) for each in args)
                elif empty_string_to_none:
                    args = tuple({key: value if value != '' else None for key, value in each.items()} for each in args)
            else:
                if args_to_dict:
                    if keys is None:
                        keys = tuple(map(str, range(1, len(args) + 1)))
                        is_key_generated = True
                    if isinstance(args[0], str):
                        args = ({key: value if value != '' else None for key, value in zip(keys, args)},
                                ) if empty_string_to_none else (dict(zip(keys, args)),)
                    elif not hasattr(args[0], '__getitem__'):
                        if hasattr(args[0], '__iter__'):  # list[set, Generator, range]
                            # mysqlclient, pymysql均只支持dict, list, tuple，不支持set, Generator等
                            args = tuple({key: value if value != '' else None for key, value in zip(keys, each)}
                                         for each in args) if empty_string_to_none else tuple(
                                dict(zip(keys, each)) for each in args)
                        else:  # list[int, etc.]
                            args = ({key: value if value != '' else None for key, value in zip(keys, args)},
                                    ) if empty_string_to_none else (dict(zip(keys, args)),)
                    else:
                        args = tuple({key: value if value != '' else None for key, value in zip(keys, each)} for each in
                                     args) if empty_string_to_none else tuple(dict(zip(keys, each)) for each in args)
                elif nums is not None:
                    if isinstance(args[0], str):
                        args = (tuple(args[num] if args[num] != '' else None for num in nums),
                                ) if empty_string_to_none else (tuple(map(args.__getitem__, nums)),)
                    elif not hasattr(args[0], '__getitem__'):
                        if hasattr(args[0], '__iter__'):  # list[set, Generator, range]
                            # mysqlclient, pymysql均只支持dict, list, tuple，不支持set, Generator等
                            args = tuple(tuple(each[num] if each[num] != '' else None for num in nums) for each in
                                         map(tuple, args)) if empty_string_to_none else tuple(
                                tuple(map(each.__getitem__, nums)) for each in map(tuple, args))
                        else:  # list[int, etc.]
                            args = (tuple(args[num] if args[num] != '' else None for num in nums),
                                    ) if empty_string_to_none else (tuple(map(args.__getitem__, nums)),)
                    else:
                        args = tuple(tuple(each[num] if each[num] != '' else None for num in nums) for each in args
                                     ) if empty_string_to_none else tuple(
                            tuple(map(each.__getitem__, nums)) for each in args)
                elif isinstance(args[0], str):
                    args = (tuple(each if each != '' else None for each in args),) if empty_string_to_none else (args,)
                elif not hasattr(args[0], '__getitem__'):
                    if hasattr(args[0], '__iter__'):  # list[set, Generator, range]
                        # mysqlclient, pymysql均只支持dict, list, tuple，不支持set, Generator等
                        args = tuple(tuple(e if e != '' else None for e in each) for each in args
                                     ) if empty_string_to_none else tuple(tuple(each) for each in args)
                    else:  # list[int, etc.]
                        args = (tuple(each if each != '' else None for each in args),) if empty_string_to_none else (
                            args,)
                elif empty_string_to_none:
                    args = tuple(tuple(e if e != '' else None for e in each) for each in args)
        return args if not get_info else (args, to_multiple, is_key_generated)


class OutputFormatterMixin(object):
    def format(self, query: str, args: Any, raise_error: Optional[bool] = None) -> str:
        try:
            if args is None:
                return query
            if isinstance(args, dict):
                new_args = dict((key, self.connection.literal(item)) for key, item in args.items())
                return query % new_args if '%' in query else query.format(**new_args)
            new_args = tuple(map(self.connection.literal, args))
            return query % new_args if '%' in query else query.format(*new_args)
        except Exception as e:
            if raise_error or raise_error is None and self.raise_error:
                raise e
            return query
