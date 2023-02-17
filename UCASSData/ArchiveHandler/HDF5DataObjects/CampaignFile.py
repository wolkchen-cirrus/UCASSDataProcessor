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

    This is essentially a wrapper for an HDF5 file.

    :param fn: filename
    :param mode: file open mode
    """
    def __init__(self, fn: str, mode: str = 'r'):
        self.__fn = None
        self.fn = fn

        self.__mode = None
        self.mode = mode

        self.__dd: H5dd
        self.__f = None

        self.h5ver: str = ch.getval('h5ver')

        print(f'File check returns {bool(self)}')

    def __bool__(self):
        return self.__file_check()

    def __repr__(self):
        return f'{self.fn} with {len(self.__groups())} groups'

    def __enter__(self):
        self.__f = h5.File(self.fn, self.mode, libver=('earliest', self.h5ver))
        return self.__f

    def __exit__(self):
        self.__f.close()

    def write(self):
        if bool(self):
            raise RuntimeError(f"File check is {bool(self)}")

    def read(self):
        pass

    def __groups(self):
        return self.__f.keys()

    def __datasets(self):
        ds = []
        for gn in self.__groups():
            pass

    def __file_check(self):
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
