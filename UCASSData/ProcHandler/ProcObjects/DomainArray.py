from numpy import matrix as mt
from ..ArchiveHandler.GenericDataObjects.MatrixColumn import MatrixColumn


class DomainArray(MatrixColumn):
    """non-col object where the domain is not time"""

    def __init__(self, name: str, val: mt, domain_name: str):
        self.val: mt = val
        self.dname = domain_name
        self.name: str = name
        field = self.__search_flags(self.name)
        dfield = self.__search_flags(self.dname)
        self.unit: str = field['unit']
        self.dunit: str = dfield['unit']
        self.desc: str = field['desc']
        self.ddesc: str = dfield['desc']
        self.__self_check(2)

    @property
    def domain(self):
        return self.val[:, 0]

    @property
    def range(self):
        return self.val[:, 1]
