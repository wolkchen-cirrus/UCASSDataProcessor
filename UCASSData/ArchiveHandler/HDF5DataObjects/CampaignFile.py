import os.path
import h5py as h5
from datetime import datetime as dt
from .H5dd import H5dd
from ....UCASSData import ConfigHandler as ch


class CampaignFile(object):
    """
    Main object for data storage and manipulation with hdf5; HDF5 structure is
    as follows:

    Campaign    -> Flight 1     -> Dataframe 1 | Metadata 1
                                -> Dataframe 2 | Metadata 2
                -> Flight 2     -> Dataframe 1 | Metadata 1

    This is essentially a wrapper for an HDF5 file.

    :param fn: filename
    :param mode: file open mode
    """

    def __init__(self, fn: str, mode: str = 'r'):
        self.__fn = None
        self.fn = fn

        self.__mode = None
        self.mode = mode

        self.__dd: H5dd | None = None
        self.__f = None

        self.h5ver: str = ch.getval('h5ver')

        print(f'File check returns {bool(self)}')

    def __bool__(self):
        return self.__file_check()

    def __repr__(self):
        return f'{self.fn} with {len(self.__groups())} groups'

    def __enter__(self):
        self.__f = h5.File(self.fn, self.mode, libver=('earliest', self.h5ver))
        return self

    def __exit__(self):
        self.__f.close()

    def set(self, val: H5dd):
        self.__dd = val

    def write(self, val: H5dd = None):
        if val:
            self.__dd = val
        elif not self.__dd:
            raise AttributeError("data dict not set")
        elif not bool(self):
            raise RuntimeError(f"File check is {bool(self)}")
        dat = self.__dd.__get__()

    def read(self):
        pass

    def __groups(self, group: str | list = None) -> list:
        """returns hdf5 groups, acts as check if group input specified"""
        if group:
            group = [group] if isinstance(group, str) else group
            group = [x if x in self.__f.keys() else None for x in group]
            if None in group:
                raise ValueError
            else:
                return group
        else:
            return list(self.__f.keys())

    def __datasets(self, group: str | list = None) -> dict:
        """returns hdf5 datasets for a single group or list thereof"""
        return {gn: list(self.__f[gn].keys()) for gn in self.__groups(group)}

    def __file_check(self) -> bool:
        """Returns false if file is invalid or does not exist"""
        if not os.path.exists(self.fn):
            return False
        for gn in self.__groups():
            try:
                dt.strptime(gn, ch.getval("groupDTformat"))
            except ValueError:
                return False
        return True

    @property
    def fn(self):
        return self.__fn

    @fn.setter
    def fn(self, val):
        if os.path.isabs(val):
            self.__fn = val
        else:
            raise ValueError("must be abs path")

    @property
    def mode(self):
        return self.__mode

    @mode.setter
    def mode(self, val):
        if val not in ['r', 'r+', 'w', 'w-', 'x', 'a']:
            raise ValueError
        else:
            self.__mode = val
