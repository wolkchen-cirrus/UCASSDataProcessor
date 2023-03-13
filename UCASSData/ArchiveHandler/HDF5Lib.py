"""
Contains functions to create, manage, append to, and report HDF5 data files for
UCASS data. Each measurement campaign is a file, each flight/instance during
the campaign is a group, which has varying relevant datasets/metadata.
"""

import h5py
from datetime import datetime as dt
import numpy as np


def dict_to_dset(dat: list[dict] | dict, grp: h5py.Group) -> h5py.Dataset:
    """
    turns dicts into h5 groups and writes them to specified pointer; ensures
    correct formatting.
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
            elif isinstance(v, dt):
                v = (v - dt(1970, 1, 1)).total_seconds()
                lv = 1
            elif hasattr(v, "__len__"):
                v = np.asarray(v, dtype=float)
                lv = v.shape
            else:
                print(f"skipping {v}")
                continue
            length.append(lv)
            names.append(k)
            vals.append(v)
    return [grp.create_dataset(n, s, data=v)
            for n, v, s in zip(names, vals, length)][0]
