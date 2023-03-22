from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ...ArchiveHandler import ImportLib as im
from .__UnitArray import UnitArray as ua

from datetime import datetime as dt
from typing import final
import pandas as pd
import numpy as np


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({dt.now()})', *args, **kwargs)


print = timestamped_print


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

    def __getcols(self, tags: list | str):
        """Gets columns with specified tags"""
        if isinstance(tags, str):
            tags = [tags]
        [im.check_flags(x) for x in tags]
        df = self.di.df()[tags]
        return self.__df_to_ual(df)

    def __df_to_ual(self, df: pd.DataFrame) -> list:
        dfd = df.to_dict(orient='list')
        return [ua(k, np.matrix(v).T, len(v)) for k, v in dfd.items()]

    @final
    def __self_check(self):
        """ensures valid subclass (placeholder)"""
        return

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
        self.__do = val
