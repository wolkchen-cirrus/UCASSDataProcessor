from .MatrixColumn import MatrixColumn
from .DataStruct import DataStruct
from numpy import matrix as mt


class ImportObject(DataStruct):
    """
    Object to provide import checking and protection, used by HDF5 modulator.
    """
    def init(self, dat: dict):
        for k, v in dat.items():
            if isinstance(v, mt):
                self.col_dict[k] = MatrixColumn(k, v, len(self))
            else:
                print("%s cannot be assigned to matrix column" % k)

        self._self_check()
