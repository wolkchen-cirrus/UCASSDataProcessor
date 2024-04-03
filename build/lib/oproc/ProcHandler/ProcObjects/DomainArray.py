from numpy import matrix as mt
from .. import ImportLib as im


class DomainArray(object):
    """non-col object where the domain is not time"""

    def __init__(self, name: str, val: mt, domain_name: str, domain: mt):
        self.val: __DomRan = __DomRan(val[:, 0], val[:, 1])
        self.name: __DomRan = __DomRan(domain_name, val)
        field = self.__search_flags(self.name)
        dfield = self.__search_flags(self.dname)
        self.unit: __DomRan = __DomRan(dfield['unit'], field["unit"])
        self.desc: __DomRan = __DomRan(dfield['desc'], field["desc"])

    @staticmethod
    def __search_flags(flag: str) -> dict:
        return im.check_flags(flag, rt=True)

    @property
    def domain(self):
        return self.val.domain

    @property
    def range(self):
        return self.val.range


class __DomRan(object):
    """
    Object to define a domain, range pair. Can be names, descriptions, units,
    or values
    """

    def __init__(self, d, r):
        self.__domain = None
        self.__range = None

    @staticmethod
    def __match_types(v, ref):
        if not ref:
            return v
        elif isinstance(v, type(ref)):
            return v
        else:
            raise TypeError

    @staticmethod
    def __match_len(v, ref):
        if isinstance(v, str):
            return v
        elif len(v) == len(ref):
            return v
        else:
            raise ValueError

    @property
    def domain(self):
        return self.__domain

    @domain.setter
    def domain(self, val):
        self.__domain = self.match_len(self.__match_types(val, self.range),
                                       self.range)

    @property
    def range(self):
        return self.__range

    @range.setter
    def range(self, val):
        self.__range = self.match_len(self.__match_types(val, self.domain),
                                      self.domain)
