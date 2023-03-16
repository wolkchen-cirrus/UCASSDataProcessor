from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ...ArchiveHandler import ImportLib as im
from .__UnitArray import UnitArray as ua

from datetime import datetime as dt
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

    def __int__(self, di: md):
        self.__di = None
        for cls in reversed(self.__class__.mro()):
            if hasattr(cls, 'init'):
                cls.init(self, di)

    def init(self, di: md):
        """
        Called by __init__().

        :param di: MatrixDict object and data input.

        :return: processed data defined by __proc()
        """
        self.di = di
        return self.__proc()

    def __proc(self):
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
        return [ua(k, np.matrix(v).T, len(self.__di)) for k, v in dfd.items()]

    @property
    def di(self):
        return self.__di

    @di.setter
    def di(self, val):
        if not isinstance(val, md):
            raise TypeError
        self.__di = val
