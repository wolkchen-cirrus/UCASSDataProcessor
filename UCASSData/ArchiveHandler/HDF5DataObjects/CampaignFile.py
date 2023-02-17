import os.path
import h5py as h5
from datetime import datetime as dt
from .H5dd import H5dd
from UCASSData import ConfigHandler as ch


class CampaignFile(object):
    """
    Main object for data storage and manipulation with hdf5; HDF5 structure is
    as follows:

    Campaign    -> Flight 1     -> Dataframe 1 | Metadata 1
                                -> Dataframe 2 | Metadata 2
                -> Flight 2     -> Dataframe 1 | Metadata 1

    """
    def __init__(self, dd: H5dd, fn: str = None, w: bool = False):
        self.__fn = None
        self.fn = fn

        self.h5ver: str = ch.getval('h5ver')
        self.dd = dd
        self.w = w

        self.__f = None

        self._file_check()

    def __add__(self, other):
        if isinstance(other, CampaignFile):
            self.dd.__add__(other.dd)
        elif isinstance(other, H5dd):
            self.dd.__add__(other)
        return self

    def __delitem__(self, key):
        self.dd.__delitem__(key)

    def __bool__(self):
        return self._file_check()

    def __repr__(self):
        return f'{self.fn} with {len(self.__groups())} groups'

    def __enter__(self):
        self.__open_file('rw')
        return self.__f

    def __exit__(self):
        self.__close_file()

    def _file_check(self):
        if not os.path.exists(self.fn):
            return False
        gns = self.__groups()
        for gn in gns:
            try:
                dt.strptime(gn, ch.getval("groupDTformat"))
            except ValueError:
                raise AttributeError("1 or more bad HDF5 groups")

    def save(self):
        if os.path.exists(self.fn):
            if self.w:
                print(f"File at {self.fn} exists, overwriting")
            else:
                raise FileExistsError
        else:
            self.__open_file('w')
            # TODO: figure this out
            self.__close_file()

    def __open_file(self, mode):
        self.__f = h5.File(self.fn, mode, libver=('earliest', self.h5ver))

    def __close_file(self):
        self.__f.close()

    def __groups(self):
        self.__open_file('r')
        k = self.__f.keys()
        self.__close_file()
        return k

    @property
    def fn(self):
        return self.__fn

    @fn.setter
    def fn(self, val):
        if os.path.isabs(val):
            self.__fn = val
        elif val is None:
            print("No file path specified")
        else:
            raise ValueError("must be abs path")
