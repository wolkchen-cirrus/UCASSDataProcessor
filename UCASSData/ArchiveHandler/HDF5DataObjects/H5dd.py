from datetime import datetime as dt
from ....UCASSData import ConfigHandler as ch
from ..GenericDataObjects.MatrixDict import MatrixDict as md


class H5dd(object):
    def __init__(self, matrix_dict: list[md]):
        self.md: list[md] = matrix_dict
        self.date_times: list[dt] = [x.__get__()['date_time'] for x in self.md]
        self.__gn: list[str] = [x.strftime(ch.getval("groupDTformat"))
                                for x in self.date_times]
        print(f'Creating HDF5 dict with group(s) {self.gn}')

    def __add__(self, other):
        self.md.append(other.md)
        self.__gn.append(other.gn)
        return self

    def __delitem__(self, key):
        self.md.__delitem__(key)

    def __len__(self):
        return len(self.md)

    def __repr__(self):
        return f'H5dd({len(self)})'

    def __get__(self):
        return self.md

    @property
    def gn(self):
        return self.__gn

    @gn.repr
    def gn(self):
        return f'size: {len(self.__gn)}, groups: {self.__gn}'
