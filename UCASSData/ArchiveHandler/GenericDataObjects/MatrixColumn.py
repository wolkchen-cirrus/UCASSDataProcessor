from numpy import matrix as mt
import re
from ... import ConfigHandler as ch
from datetime import datetime


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({datetime.now()})', *args, **kwargs)


print = timestamped_print


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

    def __get__(self) -> mt:
        return self.val

    def __repr__(self):
        return f'DataStruct({len(self)}, {self.name})'

    @staticmethod
    def _search_flags(flag: str) -> dict:
        try:
            flag = flag.replace(re.search(r'(?=\d)\w+', flag).group(), '#')
        except AttributeError:
            pass
        field = [x for x in ch.getval('valid_flags') if x['name'] == flag]
        if len(field) != 1:
            raise LookupError
        else:
            return field[0]
