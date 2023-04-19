from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ...ArchiveHandler import ImportLib as im
from ... import newprint
from .__UnitArray import UnitArray as ua

from typing import final
import pandas as pd
import numpy as np
import ast
import inspect


# Redefining print function with timestamp
print = newprint()


class Proc(object):
    """
    Base processor object. User-defined processes must be subclasses of this
    object.

    :param di: MatrixDict object and data input.
    """

    @final
    def __init__(self, di: md, **kwargs):

        self.__di = None
        self.__do = None

        self.__self_check()

        self.di = di
        self.do = self.__proc(**kwargs)

        return self.do

    def __proc(self, **kwargs):
        """Designed to be overwritten by subclass"""
        print("Undefined proc, returning input")
        return self.di

    def __getcols(self, tags: list | str) -> dict:
        """Gets columns with specified tags, must be in col_dict"""
        if isinstance(tags, str):
            tags = [tags]
        [im.check_flags(x) for x in tags]
        df = self.di.df()
        tags = [im.tag_generic_to_numeric(x, df.columns) for x in tags]
        df = df[tags]
        return self.__df_to_ual(df)

    def __df_to_uad(self, df: pd.DataFrame) -> dict:
        dfd = df.to_dict(orient='list')
        return {k: ua(k, np.matrix(v).T, len(v)) for k, v in dfd.items()}

    @final
    def __self_check(self):
        """ensures valid subclass"""
        if any(isinstance(node, ast.Return) for node in
               ast.walk(ast.parse(inspect.getsource(self.__proc)))) is False:
            raise AttributeError("return not implemented in __proc")

    def __len__(self):
        return len(self.__di)

    @property
    def di(self):
        """data input"""
        return self.__di

    @di.setter
    def di(self, val):
        if not isinstance(val, md):
            raise TypeError
        self.__di = val

    @property
    def do(self):
        """data output"""
        return self.__do

    @do.setter
    def do(self, val):
        if not isinstance(val, md):
            raise TypeError
        self.__do = self.di + val
