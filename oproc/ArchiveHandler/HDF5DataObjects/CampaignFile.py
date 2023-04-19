import os.path
import h5py as h5
from .. import HDF5Lib as h5l
from datetime import datetime as dt
from .H5dd import H5dd
from ... import ConfigHandler as ch
from ... import newprint


# Redefining print function with timestamp
print = newprint()


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
        return f'{self.fn}, Groups({len(self.__groups())}), \
                 Data({len(self.__datasets)})'

    def __enter__(self):
        self.__f = h5.File(self.fn, self.mode, libver=('earliest', self.h5ver))
        return self

    def __exit__(self, type, val, trace):
        self.__f.close()

    def set(self, val: H5dd):
        if self.__dd is None:
            self.__dd = val
        else:
            self.__dd = self.__dd + val

    def write(self, val: H5dd | None):
        if val:
            self.__dd = val
        elif not self.__dd:
            raise AttributeError("data dict not set")
        elif bool(self):
            print(f"File check is {bool(self)}")
            if ("w" not in self.mode) or ("a" not in self.mode):
                raise AttributeError("Wrong mode to create file")
            if "w" in self.mode:
                while True:
                    ui = input(f"Write over file {self.fn}? (y/n)")
                    if ui == "n":
                        print("Aborting write")
                        raise FileExistsError
                    elif ui == "y":
                        break
                    else:
                        print(f"Invalid option {ui}")
                        continue
        elif self.mode == "r":
            raise AttributeError("File opened in read mode, cannot write")
        df = self.__dd.df()
        dfm = self.__dd.df_meta()
        nc = self.__dd.non_col()
        ncm = self.__dd.nc_meta()
        wg = self.__dd.gn
        gm = self.__dd.gm()
        for g in wg:
            try:
                self.__groups(g)
                raise FileExistsError
            except ValueError:
                pass
        print(f"Writing groups {wg} to file {self.fn}")
        [self.__f.create_group(g) for g in wg]
        for g in wg:
            group = self.__f[g]
            df_group = group.create_group("columns")
            nc_group = group.create_group("extras")

            print(f'Writing dataset to group {group}')
            ds = df_group.create_dataset("dataframe", df[g].shape, data=df[g])

            print(f'Writing metadata to dataset {ds}')
            ug = df_group.create_group("units")
            dg = df_group.create_group("descriptions")
            h5l.metadict_to_attrs(dfm[g][0], ug)
            h5l.metadict_to_attrs(dfm[g][1], dg)

            print(f'Writing extra datasets in group {group}')
            h5l.dict_to_dset(nc[g], nc_group)

            print(f'Writing metadata to extra datasets')
            ug = nc_group.create_group("units")
            dg = nc_group.create_group("descriptions")
            h5l.metadict_to_attrs(ncm[g][0], ug)
            h5l.metadict_to_attrs(ncm[g][1], dg)

            print(f'Writing attributes to group {group}')
            h5l.metadict_to_attrs(gm[g].__dict__(), group)

    def read(self):
        if self.mode not in ['r', 'r+']:
            raise ValueError("h5 file not opened in read mode")

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
            try:
                return list(self.__f.keys())
            except AttributeError:
                return []

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
            proc_dir = ch.getval("base_data_path")
            self.__fn = os.path.join(proc_dir, "Processed", val)

    @property
    def mode(self):
        return self.__mode

    @mode.setter
    def mode(self, val):
        if val not in ['r', 'r+', 'w', 'w-', 'x', 'a']:
            raise ValueError
        else:
            self.__mode = val
