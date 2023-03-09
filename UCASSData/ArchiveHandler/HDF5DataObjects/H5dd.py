from datetime import datetime as dt
import re
from ....UCASSData import ConfigHandler as ch
from ..GenericDataObjects.MatrixDict import MatrixDict as md
from ..RawDataObjects.MetaDataObject import MetaDataObject as meta


class H5dd(object):
    """
    HDF5 data to be assigned to a file. Each md instance in list is one group
    """

    def __init__(self, matrix_dict: list[md] | md,
                 group_meta: list[meta] | meta):
        if not isinstance(matrix_dict, list):
            matrix_dict = [matrix_dict]
        elif not isinstance(matrix_dict, md):
            raise TypeError

        if not isinstance(group_meta, list):
            group_meta = [group_meta]
        elif not isinstance(group_meta, meta):
            raise TypeError

        if len(group_meta) != len(matrix_dict):
            raise ValueError

        self.group_meta = group_meta
        self.md: list[md] = matrix_dict
        self.date_times: list[dt] = [x.__get__()['date_time'] for x in self.md]
        self.__gn: list[str] = [x.strftime(ch.getval("groupDTformat"))
                                for x in self.date_times]
        print(f'Creating HDF5 dict with group(s) {self.gn}')

    def __add__(self, other):
        self.__group_meta.append(other.group_meta)
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

    def df(self) -> dict:
        p = str(ch.getval("timestep")) + "S"
        return {g: x.df(period=p) for g, x in zip(self.gn, self.md)}

    def df_meta(self) -> dict:
        df = self.df()
        cols = {g: x.columns for g, x in df.items()}
        meta = {}
        for g, cl in cols.items():
            flags = []
            for flag in cl:
                try:
                    flag = flag.replace(re.search(r'(?=\d)\w+', flag)
                                        .group(), '#')
                except AttributeError:
                    pass
                flags.append([x for x in ch.getval("valid_flags")
                              if x["name"] == flag][0])
            meta[g] = flags
        return meta

    def non_col(self) -> dict:
        return {g: x.non_col for g, x in zip(self.gn, self.md)}

    def gm(self) -> dict:
        meta = {}
        for m in self.group_meta:
            g = dt.strptime(m.date_time, ch.getval("groupDTformat"))
            if g not in self.gn:
                raise AttributeError
            meta[g] = m
        return meta

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
