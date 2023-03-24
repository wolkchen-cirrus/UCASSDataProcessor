from numpy import matrix as mt
import re
from ... import ConfigHandler as ch


class MatrixColumn(object):
    """
    Defines metadata for matrix column
    """

    def __init__(self, name: str | None, val: mt, dlen: int):
        self.val: mt = val
        self.dlen: int = dlen
        if name:
            self.name: str = name
            field = self.__search_flags(self.name)
            self.unit: str = field['unit']
            self.desc: str = field['desc']
            self.__self_check()
        else:
            self.name = None
            self.unit = None
            self.desc = None

    def __self_check(self):
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

    def __get__(self) -> mt:
        return self.val

    def __repr__(self):
        return f'DataStruct({len(self)}, {self.name})'

    @staticmethod
    def __search_flags(flag: str) -> dict:
        try:
            flag = flag.replace(re.search(r'(?=\d)\w+', flag).group(), '#')
        except AttributeError:
            pass
        field = [x for x in ch.getval('valid_flags') if x['name'] == flag]
        if len(field) != 1:
            raise LookupError
        else:
            return field[0]
