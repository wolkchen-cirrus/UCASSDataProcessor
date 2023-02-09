import numpy as np
import pandas as pd

from .MatrixColumn import MatrixColumn
from .DataStruct import DataStruct


class MatrixDict(DataStruct):
    """Intermediate step for data import process"""

    def init(self, dat: dict):

        self.non_col: dict = {}

        for k, v in dat.items():
            if k == "Time":
                pass
            elif isinstance(v, np.matrix):
                self.col_dict[k] = MatrixColumn(k, v, len(self))
            else:
                self.non_col[k] = v

    def _self_check(self):
        print("Self check not implemented for MatrixDict")
        pass

    def __get__(self) -> dict:
        """return the dict plus non col values combined"""
        return self.col_dict | self.non_col | {"Time": self.Time}
