from datetime import datetime as dt
from ... import ConfigHandler as ch
from ..GenericDataObjects.MatrixDict import MatrixDict as md
from ..RawDataObjects.MetaDataObject import MetaDataObject as meta


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({dt.now()})', *args, **kwargs)


print = timestamped_print


class H5dd(object):
    """
    HDF5 data to be assigned to a file. Each md instance in list is one group
    """

    def __init__(self, matrix_dict: list[md] | md | None,
                 group_meta: list[meta] | meta | None):

        if (matrix_dict is None) and (group_meta is None):
            print("Creating blank H5dd")
            self.group_meta = []
            self.md = []
            self.__gn = []
            self.date_times = []
        elif (matrix_dict is None) or (group_meta is None):
            raise ValueError("Both must be None")
        else:

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
            self.date_times: list[dt] = [x.__get__()['date_time']
                                         for x in self.md]
            self.__gn: list[str] = [x.strftime(ch.getval("groupDTformat"))
                                    for x in self.date_times]
            print(f'Creating HDF5 dict with group(s) {self.gn}')

    def __add__(self, other):
        self.group_meta = self.group_meta + other.group_meta
        self.md = self.md + other.md
        self.__gn = self.__gn + other.gn
        self.date_times = self.date_times + other.date_times
        self.__check_gn()
        return self

    def __len__(self):
        return len(self.md)

    def __repr__(self):
        return f'Items({len(self)}), Groups({self.__gn})'

    def __get__(self):
        return {g: x.__get__() for g, x in zip(self.gn, self.md)}

    def df(self) -> dict:
        """main col data"""
        p = str(ch.getval("timestep")) + "S"

        def __df(mat_d):
            df = mat_d.df(period=p)
            df['Time'] = [(d - dt(1970, 1, 1)).total_seconds()
                          for d in df.index]
            return df.to_records(index=False)

        return {g: __df(x) for g, x in zip(self.gn, self.md)}

    def df_meta(self) -> dict:
        """main col dataframe attributes; group: ({units}, {descriptions})"""
        vf = ch.getval('valid_flags')
        out = {}
        for g, m in zip(self.gn, self.md):
            desc = [x for x in vf if x["name"] in list(m.col_dict.keys())]
            unit = {x['name']: x['unit'] for x in desc}
            desc = {x['name']: x['desc'] for x in desc}
            out[g] = (unit, desc)
        return out

    def non_col(self) -> dict:
        """Non col HDF dataframes"""
        return {g: x.non_col for g, x in zip(self.gn, self.md)}

    def nc_meta(self) -> dict:
        """non-col attributes; group: ({units}, {descriptions})"""
        vf = ch.getval('valid_flags')
        out = {}
        for g, m in zip(self.gn, self.md):
            desc = [x for x in vf if x["name"] in list(m.non_col.keys())]
            unit = {x['name']: x['unit'] for x in desc}
            desc = {x['name']: x['desc'] for x in desc}
            out[g] = (unit, desc)
        return out

    def gm(self) -> dict:
        """Group metadata"""
        meta = {}
        for m in self.group_meta:
            g = m.date_time.strftime(ch.getval("groupDTformat"))
            if g not in self.gn:
                raise AttributeError(f'{g} is not in {self}')
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
