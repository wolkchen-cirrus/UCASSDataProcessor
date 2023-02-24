import datetime as dt
import pandas as pd
import numpy as np
from .MatrixColumn import MatrixColumn
from ... import ConfigHandler as ch


class DataStruct(object):
    """
    Template data structure
    """
    def __init__(self, dat: dict, unit_spec: dict = None):
        self.__unit_spec = None
        self.__col_dict = None
        self.__date_time = None
        self.__Time = None
        for cls in reversed(self.__class__.mro()):
            if hasattr(cls, 'init'):
                cls.init(self, dat, unit_spec=unit_spec)

    def init(self, dat: dict, unit_spec: dict = None):
        self.unit_spec = unit_spec
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

    def df(self, period=None) -> pd.DataFrame:
        """Returns matrix columns as a dataframe"""
        self._self_check()
        md = dict([(k, np.squeeze(np.array(v.__get__())))
                   for k, v in self.col_dict.items()])
        md = md | {"Time": self.Time}
        if period:
            return pd.DataFrame.from_dict(md).set_index('Time', drop=True)\
                .resample(period).mean().bfill()
        else:
            return pd.DataFrame.from_dict(md).set_index('Time', drop=True)

    def __repr__(self):
        return f'DataStruct({len(self)}, {len(self.col_dict)})'

    def __bool__(self):
        return bool(self.col_dict)

    @property
    def unit_spec(self):
        return self.__unit_spec

    @unit_spec.setter
    def unit_spec(self, val):
        if val is None:
            return
        for k, v in val.items():
            if not isinstance(v, str):
                raise TypeError
            elif k not in [x['name'] for x in ch.getval('valid_flags')]:
                raise LookupError
        self.__unit_spec = val

    @property
    def col_dict(self):
        return self.__col_dict

    @col_dict.setter
    def col_dict(self, val):
        if not val:
            self.__col_dict = val
        for k, v in val.items():
            if not isinstance(v, MatrixColumn):
                raise TypeError
            elif k not in [x['name'] for x in ch.getval('valid_flags')]:
                raise LookupError
        self.__col_dict = val

    @property
    def date_time(self):
        """Date and time at start"""
        return self.__date_time

    @date_time.setter
    def date_time(self, val):
        if isinstance(val, dt.datetime):
            self.__date_time = val
        else:
            raise TypeError('Value must be in python date_time format')

    @property
    def Time(self):
        return self.__Time

    @Time.setter
    def Time(self, val):
        if not isinstance(val, pd.DatetimeIndex):
            raise TypeError
        else:
            self.__Time = val
