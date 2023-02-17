from ..GenericDataObjects.MatrixColumn import MatrixColumn
from ..GenericDataObjects.DataStruct import DataStruct
from numpy import matrix as mt


class ImportObject(DataStruct):
    """
    Object to provide import checking and protection, used by HDF5 modulator.
    """
    def init(self, dat: dict, unit_spec: dict = None):
        if unit_spec:
            raise ValueError("do not specify unit_spec")
        col_dict = {}
        for k, v in dat.items():
            if isinstance(v, mt):
                print("converting %s to matrix column" % k)
                col_dict[k] = MatrixColumn(k, v, len(self))
            elif isinstance(v, MatrixColumn):
                col_dict[k] = v
            else:
                print("%s cannot be assigned to matrix column" % k)
        self.col_dict = col_dict

        self._self_check()

    def __dict__(self):
        return {self.col_dict | self.Time}
