# -*- coding: utf-8 -*-
"""

"""

from abc import ABCMeta, abstractmethod
from enum import auto, EnumMeta
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
