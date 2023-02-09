import datetime as dt
import pandas as pd
import numpy as np
from .MatrixColumn import MatrixColumn
import UCASSData.ConfigHandler as ch


class DataStruct(object):
    """
    Template data structure
    """
    def __init__(self, dat: dict):
        for cls in reversed(self.__class__.mro()):
            if hasattr(cls, 'init'):
                cls.init(self, dat)

    def init(self, dat: dict):
        self.col_dict: dict = {}
        self.date_time: dt.datetime = dat['date_time']
        self.Time: pd.DatetimeIndex = dat['Time']

    def __len__(self):
        """Length of time is taken as reference"""
        return self.Time.shape[0]

    def _self_check(self):
        if not self.col_dict:
            print("col_dict not populated")
        else:
            for k, v in self.col_dict.items():
                if not isinstance(v, MatrixColumn):
                    raise TypeError
                elif k not in [x['name'] for x in ch.getval('valid_flags')]:
                    raise LookupError

    def df(self) -> pd.DataFrame:
        """Returns matrix columns as a dataframe"""
        self._self_check()
        md = dict([(k, np.squeeze(np.array(v.__get__())))
                   for k, v in self.col_dict.items()])
        md = md | {"Time": self.Time}
        return pd.DataFrame.from_dict(md).set_index('Time', drop=True)
