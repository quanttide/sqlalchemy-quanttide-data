# -*- coding: utf-8 -*-
"""

"""

from abc import ABCMeta, abstractmethod
from enum import auto, EnumMeta, IntEnum
import re


class ABCEnumMeta(EnumMeta, ABCMeta):
    """
    抽象枚举类的元类。

    Ref:
      - https://stackoverflow.com/questions/56131308/create-an-abstract-enum-class
    """
    pass


class AbstractParamStyle(metaclass=ABCMeta):
    """
    参数样式的抽象枚举类。
    """
    @property
    @abstractmethod
    def pattern(self):
        pass

    @property
    @abstractmethod
    def pattern_esc(self):
        pass

    @property
    @abstractmethod
    def repl(self):
        pass

    @property
    @abstractmethod
    def repl_format(self):
        pass

    @classmethod
    def judge_paramstyle(cls, query: str, first: Optional[Paramstyle] = None) -> Optional[Paramstyle]:
        if first is not None and cls._pattern[first].search(query):
            return first
        for from_paramstyle, from_pattern in cls._pattern.items():
            if from_paramstyle != first and from_pattern.search(query):
                return from_paramstyle
        else:
            return None

    @classmethod
    def transform_paramstyle(cls, query: str, to_paramstyle: Paramstyle,
                             from_paramstyle: Union[Paramstyle, None, tuple] = ()) -> str:
        # args需预先转换为dict或list形式（与to_paramstyle相对应的那一种）
        # from_paramstyle=None: 无paramstyle; from_paramstyle=(): 未传入该参数
        if isinstance(from_paramstyle, tuple):
            from_paramstyle = cls.judge_paramstyle(query, to_paramstyle)
        if from_paramstyle is None:
            return query
        if from_paramstyle != to_paramstyle:
            if to_paramstyle == Paramstyle.numeric or to_paramstyle in (
                    Paramstyle.pyformat, Paramstyle.named) and from_paramstyle in (Paramstyle.format, Paramstyle.qmark):
                pos_count = itertools.count(1)
                query = cls._pattern[from_paramstyle].sub(
                    lambda m: cls._repl_format[to_paramstyle].format(str(next(pos_count))), query)
            else:
                query = cls._pattern[from_paramstyle].sub(cls._repl[to_paramstyle], query)
        return cls._pattern_esc[from_paramstyle].sub(r'\1', query)

    @staticmethod
    def paramstyle_formatter(arg: Union[Sequence, dict], paramstyle: Optional[Paramstyle] = None) -> Iterable[str]:
        if paramstyle is None or paramstyle == Paramstyle.format:
            return ('%s',) * len(arg)
        if paramstyle == Paramstyle.pyformat:
            return map('%({})s'.format, arg)
        if paramstyle == Paramstyle.named:
            return map(':{}'.format, arg)
        if paramstyle == Paramstyle.numeric:
            return map(':{}'.format, range(1, len(arg) + 1))
        if paramstyle == Paramstyle.qmark:
            return ('?',) * len(arg)
        raise ValueError(paramstyle)


class PyFormatStyle(AbstractParamStyle):
    pattern = re.compile(r'(?<![%\\])%\(([\w$]+)\)s')
    pattern_esc = re.compile(r'[%\\](%\([\w$]+\)s)')
    repl = r'%(\1)s'
    repl_format = r'%({})s'


class FormatStyle(AbstractParamStyle):
    pattern = re.compile(r'(?<![%\\])%s')
    pattern_esc = re.compile(r'[%\\](%s)')
    repl = '%s'
    repl_format = None


class NamedStyle(AbstractParamStyle):
    pattern = re.compile(r'(?<![:\w$\\]):([a-zA-Z_$][\w$]*)(?!:)')
    pattern_esc = re.compile(r'\\(:[a-zA-Z_$][\w$]*)(?!:)')
    repl = r':\1'
    repl_format = r':{}'


class NumericStyle(AbstractParamStyle):
    pattern = re.compile(r'(?<![:\d\\]):(\d+)(?!:)')
    pattern_esc = re.compile(r'\\(:\d+)(?!:)')
    repl = None
    repl_format = r':{}'


class QMarkStyle(AbstractParamStyle):
    pattern = re.compile(r'(?<!\\)\?')
    pattern_esc = re.compile(r'\\(\?)')
    repl = '?'
    repl_format = None


class Paramstyle(IntEnum):
    pyformat = 0
    format = 1
    named = 2
    numeric = 3
    qmark = 4
