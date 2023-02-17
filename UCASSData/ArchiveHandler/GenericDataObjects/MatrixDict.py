import numpy as np
import pandas as pd
from .MatrixColumn import MatrixColumn
from .DataStruct import DataStruct
from UCASSData.ArchiveHandler import ureg
from UCASSData import ConfigHandler as ch


class MatrixDict(DataStruct):
    """Intermediate step for data import process"""

    def init(self, dat: dict, unit_spec: dict | str = None):

        self.__out_unit = dict([(x['name'], x['unit'])
                                for x in ch.getval('valid_flags')])

        if not unit_spec:
            raise ValueError("No units specified")
        if isinstance(unit_spec, str):
            if unit_spec == "default":
                self.unit_spec = self.__out_unit
            else:
                raise ValueError
        else:
            self.unit_spec = unit_spec

        self.non_col: dict = {}

        for k, v in dat.items():
            v = self.__convert_units(k, v)
            if k == "Time":
                pass
            elif isinstance(v, np.matrix):
                self.col_dict[k] = MatrixColumn(k, v, len(self))
            else:
                self.non_col[k] = v

    def __convert_units(self, tag: str, val: np.matrix):
        if tag not in self.unit_spec:
            print("%s has no unit to convert" % tag)
            return val
        elif self.unit_spec[tag] == self.__out_unit[tag]:
            print("%s is already at correct unit" % tag)
            return val
        else:
            print("converting %s from %s to %s" %
                  (tag, self.unit_spec[tag], self.__out_unit[tag]))
            val = (val * ureg(self.unit_spec[tag]))\
                .to(ureg(self.__out_unit[tag]))
            return val.magnitude

    def _self_check(self):
        print("Self check not implemented for MatrixDict")
        pass

    def __sync2(self, other) -> dict:
        df = pd.merge(self.df(), other.df(), how='outer', left_index=True,
                      right_index=True, suffixes=(None, '_%%SUFFIX%%'))
        df = df[df.columns.drop(list(df.filter(regex='%%SUFFIX%%')))]
        dd = dict([(k, np.matrix(v).T)
                   for k, v in df.to_dict(orient='list').items()])
        dd['Time'] = df.index
        return dd

    def __get__(self) -> dict:
        """return the dict plus non col values combined"""
        return self.col_dict | self.non_col | {"Time": self.Time}

    def __iadd__(self, other):
        """append matrix dicts"""
        if not isinstance(other, MatrixDict):
            raise TypeError
        self.non_col = self.non_col | other.non_col
        dd = self.__sync2(other)
        self.Time = dd["Time"]
        dd.pop("Time", None)
        dd = dict([(k, MatrixColumn(k, v, len(self))) for k, v in dd.items()])
        self.col_dict = dd
        return self

    def __add__(self, other):
        """add matrix dicts"""
        if not isinstance(other, MatrixDict):
            raise TypeError
        non_col = self.non_col | other.non_col
        dd = self.__sync2(other)
        dd = dict([(k, MatrixColumn(k, v, len(dd["Time"])))
                   for k, v in dd.items()])
        return MatrixDict(non_col | dd)

    def __delitem__(self, key):
        try:
            self.col_dict.pop(key, None)
        except KeyError:
            self.non_col.pop(key, None)
