from numpy import matrix as mt
import UCASSData.ConfigHandler as ch


class MatrixColumn(object):
    """
    Defines metadata for matrix column
    """
    def __init__(self, name: str, val: mt, dlen: int):
        self.name: str = name
        self.val: mt = val
        field = self._search_flags(self.name)
        self.unit: str = field['unit']
        self.desc: str = field['desc']
        self.dlen: int = dlen
        self._self_check()

    def _self_check(self):
        if not isinstance(self.val, mt):
            raise TypeError
        elif self.val.shape[1] != 1:
            raise ValueError("only 1 column")
        elif len(self) != self.dlen:
            raise ValueError("Length must be %i" % self.dlen)
        else:
            for k, v in self.__dict__.items():
                if v is None:
                    raise AttributeError('%s is not set' % k)

    def __len__(self) -> int:
        return self.val.shape[0]

    @staticmethod
    def _search_flags(flag: str) -> dict:
        field = [x for x in ch.getval('valid_flags') if x['name'] == flag]
        if len(field) != 1:
            raise FileNotFoundError
        else:
            return field[0]
