from ...ArchiveHandler.GenericDataObjects.MatrixColumn import MatrixColumn
import numpy as np


class UnitArray(MatrixColumn):
    """
    Array with a unit, and mathematical opperations defined.
    """

    def __iadd__(self, other):
        self.__calc_check(UnitArray, other, eunit=True)
        self.val = self.val + other.val
        return self

    def __isub__(self, other):
        self.__calc_check(UnitArray, other, eunit=True)
        self.val = self.val - other.val
        return self

    def __add__(self, other):
        self.__calc_check(UnitArray, other, eunit=True)
        val = np.add(self.val, other.val)
        return UnitArray(self.name, val, len(self))

    def __sub__(self, other):
        self.__calc_check(UnitArray, other, eunit=True)
        val = np.subtract(self.val, other.val)
        return UnitArray(self.name, val, len(self))

    def __mul__(self, other):
        self.__calc_check(UnitArray, other)
        val = np.multiply(self.val, other.val)
        return UnitArray(None, val, len(self))

    def __truediv__(self, other):
        self.__calc_check(UnitArray, other)
        val = np.divide(self.val, other.val)
        return UnitArray(None, val, len(self))

    def __pow__(self, other):
        self.__calc_check(int, other)
        val = self.val ** other
        return UnitArray(None, val, len(self))

    def set_name(self, name: str):
        self.name: str = name
        field = self.__search_flags(self.name)
        self.unit: str = field['unit']
        self.desc: str = field['desc']
        self.__self_check()

    def mc(self):
        """return normie matrix column"""
        if not self.name:
            raise ValueError("Set name before proceeding")
        self.__self_check()
        return MatrixColumn(self.name, self.val, len(self))

    def __calc_check(self, tp, obj, eunit=False):
        """pre calculation check"""
        if not isinstance(obj, tp):
            raise NotImplementedError
        elif not self.name:
            raise ValueError("Set name before proceeding")
        elif eunit is True:
            if self.unit != obj.unit:
                raise AttributeError("Unit mismatch")
