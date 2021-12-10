# -*- coding: utf-8 -*-


class ValidatorMixin(object):
    @classmethod
    def validate_sql_client_lib(cls):
        """
        lib模块的以下属性被下列方法使用：
        lib.ProgrammingError: close
        lib.InterfaceError, lib.OperationalError: ping, try_connect, try_execute, call_proc
        lib.cursors.DictCursor: query, call_proc
        lib.connect: connect
        """
        # 检查lib模块
        assert cls.lib is not None
        # 检查lib模块属性
        assert hasattr(cls.lib, 'ProgrammingError')
        assert hasattr(cls.lib, 'InterfaceError')
        assert hasattr(cls.lib, '')
