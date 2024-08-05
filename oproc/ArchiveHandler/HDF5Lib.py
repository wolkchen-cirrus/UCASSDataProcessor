"""
Contains functions to create, manage, append to, and report HDF5 data files for
UCASS data. Each measurement campaign is a file, each flight/instance during
the campaign is a group, which has varying relevant datasets/metadata.
"""

import h5py
from datetime import datetime as dt
import numpy as np
import pandas as pd
import typing

from .. import ConfigHandler as ch
from .. import newprint


# Redefining print function with timestamp
print = newprint()


def dict_to_dset(dat: list[dict] | dict,
                 grp: h5py.Group) -> list[h5py.Dataset]:
    """
    turns dicts into h5 datasets and writes them to specified pointer; ensures
    correct formatting. Will flatten list of dicts and write all to one
    pointer.
    """

    if isinstance(dat, dict):
        dat = [dat]
    length = []
    vals = []
    names = []
    # flattening
    for d in dat:
        if not isinstance(d, dict):
            raise TypeError
        for k, v in d.items():
            if isinstance(v, str):
                if v.isnumeric():
                    v = float(v)
                    lv = 1
                else:
                    print(f"skipping {v}")
                    continue
            elif isinstance(v, (float, int)):
                v = float(v)
                lv = 1
            elif isinstance(v, dt):
                v = (v - dt(1970, 1, 1)).total_seconds()
                lv = 1
#            elif isinstance(v, pd.DataFrame):
#                lv = v.shape
#                v = v
            elif hasattr(v, "__len__"):
                v = np.asarray(v, dtype=float)
                lv = v.shape
            else:
                print(f"skipping {v}")
                continue
            length.append(lv)
            names.append(k)
            vals.append(v)
    print(names)
    print(vals)
    return [grp.create_dataset(n, s, data=v)
            for n, v, s in zip(names, vals, length)]


def metadict_to_attrs(dat: dict, grp: h5py.Group):
    """
    turns dicts into h5 attributes and writes them to specified pointer;
    ensures correct formatting.
    """

    def __list_formatter(li: list) -> str:
        li = [str(x) for x in li]
        return ', '.join(li)

    for k, v in dat.items():
        if isinstance(v, list):
            val = __list_formatter(v)
        elif isinstance(v, dt):
            val = v.strftime(ch.getval("nominalDTformat"))
        else:
            # Attempt to just convert to UTF-8 string if all else fails
            val = str(v)
        grp.attrs[k] = val


