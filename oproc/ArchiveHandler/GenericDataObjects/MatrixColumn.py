from numpy import matrix as mt
from .. import ImportLib as im


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
            self.__self_check(1)
        else:
            self.name = None
            self.unit = None
            self.desc = None

    def __self_check(self, c: int):
        i = 0
        while True:
            if not isinstance(self.val, mt):
                raise TypeError
            elif self.val.shape[1] != c:
                if i != 0:
                    raise ValueError
                else:
                    self.val = self.val.T
                    i += 1
                    continue
            elif len(self) != self.dlen:
                raise ValueError(f"Length of {self.name} must be {self.dlen} not\
                                 {len(self)}")
            else:
                for k, v in self.__dict__.items():
                    if v is None:
                        raise AttributeError('%s is not set' % k)
                break

    def __len__(self) -> int:
        return self.val.shape[0]

    def __get__(self) -> mt:
        return self.val

    def __repr__(self):
        return f'MatrixColumn({len(self)}, {self.name})'

    @staticmethod
    def __search_flags(flag: str) -> dict:
        return im.check_flags(flag, rt=True)
