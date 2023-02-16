import numpy as np
from .MatrixColumn import MatrixColumn
from .DataStruct import DataStruct
from ...ArchiveHandler import ureg
from UCASSData import ConfigHandler as ch


class MatrixDict(DataStruct):
    """Intermediate step for data import process"""

    def init(self, dat: dict, unit_spec: dict = None):

        if not unit_spec:
            raise ValueError("No units specified")
        else:
            self.__unit_spec = unit_spec

        self.non_col: dict = {}
        self.__out_unit = dict([(x['name'], x['unit'])
                                for x in ch.getval('valid_flags')])

        for k, v in dat.items():
            v = self.__convert_units(k, v)
            if k == "Time":
                pass
            elif isinstance(v, np.matrix):
                self.col_dict[k] = MatrixColumn(k, v, len(self))
            else:
                self.non_col[k] = v

    def __convert_units(self, tag: str, val: np.matrix):
        if tag not in self.__unit_spec:
            print("%s has no unit to convert" % tag)
            return val
        elif self.__unit_spec[tag] == self.__out_unit[tag]:
            print("%s is already at correct unit" % tag)
            return val
        else:
            print("converting %s from %s to %s" %
                  (tag, self.__unit_spec[tag], self.__out_unit[tag]))
            val = (val * ureg(self.__unit_spec[tag]))\
                .to(ureg(self.__out_unit[tag]))
            return val.magnitude

    def _self_check(self):
        print("Self check not implemented for MatrixDict")
        pass

    def __get__(self) -> dict:
        """return the dict plus non col values combined"""
        return self.col_dict | self.non_col | {"Time": self.Time}
