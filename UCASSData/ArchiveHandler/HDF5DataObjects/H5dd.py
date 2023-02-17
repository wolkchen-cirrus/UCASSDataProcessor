from datetime import datetime as dt
from UCASSData import ConfigHandler as ch


class H5dd(object):
    def __init__(self, dd: dict):
        self.dd = dd
        self.date_time: dt = self.dd['date_time']
        self.__gn = self.date_time.strftime(ch.getval("groupDTformat"))
        print(f'Creating HDF5 dict with group {self.gn}')

    def __add__(self, other):
        self.dd = {self.dd | other.dd}
        return self

    def __delitem__(self, key):
        self.dd.pop(key, None)

    def __len__(self):
        return len(self.dd)

    def __repr__(self):
        return f'H5dd({len(self)})'

    @property
    def gn(self):
        return self.__gn
