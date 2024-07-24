from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
import pandas as pd
import numpy as np
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md


# Redefining print function with timestamp
print = newprint()


class NConc(Proc):

    def setup(self):
        self.ivars = ['C#', 'sample_volume']
        self.ovars = ['number_conc']
        self.unit_spec = {'number_conc': 'm**-3'}

    def proc(self):
        data = self.di.__get__()
        counts = pl.get_all_suffix('C#', data)
        counts = [v.__get__() for k, v in counts.items()]
        counts = np.concatenate(counts, axis=1)
        counts = np.sum(counts, axis=1)
        sv = data["sample_volume"]
        nc = np.divide(counts, sv.__get__())
        self.do = md({"number_conc": nc, "date_time": data["date_time"],
                     "Time": data["Time"]},\
                     unit_spec="default")
        return self.do

    def __repr__(self):
        return "NConc"
