from datetime import datetime as dt
from UCASSData import ConfigHandler as ch
from ..GenericDataObjects.MatrixDict import MatrixDict as md


class H5dd(object):
    def __init__(self, matrix_dict: md):
        self.md: md = matrix_dict
        self.date_time: dt = self.md.__get__()['date_time']
        self.__gn = self.date_time.strftime(ch.getval("groupDTformat"))
        print(f'Creating HDF5 dict with group {self.gn}')

    def __add__(self, other):
        self.md = self.md + other.dd
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
