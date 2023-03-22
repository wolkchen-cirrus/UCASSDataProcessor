from ...ArchiveHandler.GenericDataObjects.MatrixColumn import MatrixColumn
from ... import ureg

import numpy as np
import operator as op


class UnitArray(MatrixColumn):
    """
    Array with a unit, and mathematical opperations defined.
    """

    def __iadd__(self, other):
        if self.unit != other.unit:
            raise AttributeError("Unit mismatch")
        self.val = self.val + other.val
        return self

    def __isub__(self, other):
        if self.unit != other.unit:
            raise AttributeError("Unit mismatch")
        self.val = self.val - other.val
        return self

    def __mul__(self, other):
        self.unit = self.__proc_unit(self, other, op.multiply)
        self.name = ""
        self.desc = ""
        self.val = np.multiply(self.val, other.val)
        return self

    def __truediv__(self, other):
        self.unit = self.__proc_unit(self, other, op.divide)
        self.name = ""
        self.desc = ""
        self.val = np.divide(self.val, other.val)
        return self

    def __pow__(self, other):
        if not isinstance(other, int):
            raise NotImplementedError
        self.unit = self.__proc_unit(self, other, op.pow)
        self.name = ""
        self.desc = ""
        self.val = self.val ** other
        return self

    @staticmethod
    def __proc_unit(u1: str, u2: str, o: op.operator) -> str:
        return str(o(ureg(u1), ureg(u2)))
