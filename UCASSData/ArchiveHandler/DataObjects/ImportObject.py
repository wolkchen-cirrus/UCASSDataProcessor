from .MatrixColumn import MatrixColumn
from .DataStruct import DataStruct
from numpy import matrix as mt


class ImportObject(DataStruct):
    """
    Object to provide import checking and protection, used by HDF5 modulator.
    """
    def init(self, dat: dict, unit_spec: dict = None):
        if unit_spec:
            raise ValueError("do not specify unit_spec")
        for k, v in dat.items():
            if isinstance(v, mt):
                self.col_dict[k] = MatrixColumn(k, v, len(self))
            else:
                print("%s cannot be assigned to matrix column" % k)

        self._self_check()
