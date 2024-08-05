from .__Proc import Proc
from ... import newprint
from ...ArchiveHandler.RawDataObjects.RawFile import RawFile
from ...ArchiveHandler import ImportLib as im
from ...ArchiveHandler import Utilities as utils
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ... import ConfigHandler as ch

import pandas as pd
import numpy as np
import os


# Redefining print function with timestamp
print = newprint()


def get_WD_data(date_time):
    try:
        iss_path = os.environ["WD_ISS"]
    except KeyError:
        raise RuntimeError("env var WD_ISS is not set")

    issd = im.get_iss_json(iss_path, obj=False)
    types = im.types_from_iss(issd)
    fdf = utils.get_files(date_time, types, default_type='Misc',\
                          index_method='pad')
    if (len(issd) != 1) or (len(fdf) != 1):
        raise ValueError("iss must point to one file only")
    dt = fdf.index[0]
    isso = im.get_iss_obj(issd, fdf, dt)

    with RawFile(isso) as rf:
        data = rf.read()

    if len(data) != 1:
        raise ValueError("iss must point to one file only")

    data = data[list(data.keys())[0]]

    return data


class AddWD(Proc):

    def setup(self):
        self.ivars = []
        self.ovars = ['WD']
        self.unit_spec = {'WD': 'deg'}

    def proc(self):
        tvars = self.get_timevars()
        WD_arr = get_WD_data(tvars['date_time'])
        WD = WD_arr.df_dt_index(tvars['date_time'])
        self.do = {'WD': WD['WD'].to_list()[0]}
        return self.do

    def __repr__(self):
        return "AddWD"





