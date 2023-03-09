from datetime import datetime as dt
from ....UCASSData import ConfigHandler as ch
from ..GenericDataObjects.MatrixDict import MatrixDict as md


class H5dd(object):
    """
    HDF5 data to be assigned to a file. Each md instance in list is one group
    """

    def __init__(self, matrix_dict: list[md] | md):
        if not isinstance(matrix_dict, list):
            matrix_dict = [matrix_dict]
        elif not isinstance(matrix_dict, md):
            raise TypeError
        self.md: list[md] = matrix_dict
        self.date_times: list[dt] = [x.__get__()['date_time'] for x in self.md]
        self.__gn: list[str] = [x.strftime(ch.getval("groupDTformat"))
                                for x in self.date_times]
        print(f'Creating HDF5 dict with group(s) {self.gn}')

    def __add__(self, other):
        self.md.append(other.md)
        self.__gn.append(other.gn)
        self.__check_gn()
        return self

    def __len__(self):
        return len(self.md)

    def __repr__(self):
        return f'Items({len(self)}), Groups({self.gn.__repr__()})'

    def __get__(self):
        return {g: x.__get__() for g, x in zip(self.gn, self.md)}

    def df(self):
        p = str(ch.getval("timestep")) + "S"
        return {g: x.df(period=p) for g, x in zip(self.gn, self.md)}

    def df_meta(self):
        return

    def non_col(self):
        return {g: x.non_col for g, x in zip(self.gn, self.md)}

    def __check_gn(self):
        """Rasies error if invalid group"""
        if len(self.md) != len(self.gn):
            raise ValueError
        for g in self.gn:
            try:
                dt.strptime(g, ch.getval("groupDTformat"))
            except ValueError:
                raise ValueError(f"Group {g} is invalid")

    @property
    def gn(self):
        return self.__gn

    @gn.repr
    def gn(self):
        return f'size: {len(self.__gn)}, groups: {self.__gn}'
