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
        counts = pd.DataFrame.from_dict(counts, orient="columns").to_numpy()
        counts = np.sum(counts, axis=1)
        sv = data["sample_volume"]
        nc = np.divide(counts, sv)
        self.do = nc
        return self.do

    def __repr__(self):
        return "NConc"
