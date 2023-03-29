from numpy import matrix as mt
from .. import ImportLib as im


class DomainArray(object):
    """non-col object where the domain is not time"""

    def __init__(self, name: str, val: mt, domain_name: str):
        self.val: mt = val
        self.domain_name = domain_name
        self.name: str = name
        field = self.__search_flags(self.name)
        self.unit: str = field['unit']
        self.desc: str = field['desc']
        self.__self_check()

    def __self_check(self):
        if not isinstance(self.val, mt):
            raise TypeError
        elif self.val.shape[1] != 2:
            raise ValueError("2 cols: Domain, Range")
        else:
            for k, v in self.__dict__.items():
                if v is None:
                    raise AttributeError('%s is not set' % k)

    def __len__(self) -> int:
        return self.val.shape[0]

    def __get__(self) -> mt:
        return self.val

    def __repr__(self):
        return f'DomainArray({len(self)}, {self.name})'

    @staticmethod
    def __search_flags(flag: str) -> dict:
        return im.check_flags(flag, rt=True)

    @property
    def domain(self):
        return self.val[:, 0]

    @property
    def range(self):
        return self.val[:, 1]
