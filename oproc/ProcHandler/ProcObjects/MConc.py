from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from ... import ureg
import numpy as np
import pandas as pd
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md


# Redefining print function with timestamp
print = newprint()


class MConc(Proc):

    def setup(self):
        self.ivars = ['C#', 'sample_volume', 'bvs', 'density']
        self.ovars = ['mass_conc']
        self.unit_spec = {'mass_conc': 'kg m**-3'}

    def proc(self):
        data = self.get_ivars()
        tdat = self.get_timevars()
        bv_unit = ureg(self.get_input_unit('bvs'))
        density_unit = ureg(self.get_input_unit('density'))

        counts = np.concatenate([v for k, v in data.items() if 'C' in k], axis=1)
        b_mass = [(x*bv_unit).to('m**3') *
                  (data['density']*density_unit).to('kg m**-3') for x in
                  data['bvs']]
        b_mass = np.matrix(np.reshape([x.magnitude for x in b_mass], (-1, 1)))
        total_mass = np.sum(np.matmul(counts, b_mass), axis=1)
        mc = np.divide(total_mass, data['sample_volume'])
        print(mc.shape)
        self.do = md({"mass_conc": mc, "date_time": tdat["date_time"],\
                     "Time": tdat["Time"]},\
                     unit_spec="default")
        return self.do

    def __repr__(self):
        return "MConc"
