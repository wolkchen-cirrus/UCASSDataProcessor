from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from ... import ureg
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
import numpy as np
import os


# Redefining print function with timestamp
print = newprint()


class FRSampleVolume(Proc):

    def setup(self):
        self.ivars = ['sample_flow_rate', 'Period']
        self.ovars = ['sample_volume']
        self.unit_spec = {'sample_volume': 'm**3'}

    def proc(self):
        data = self.get_ivars()
        tvars = self.get_timevars()
        sfr = data["sample_flow_rate"]
        period = data["Period"]
        period_unit = ureg(self.get_input_unit('Period'))
        sfr_unit = ureg(self.get_input_unit('sample_flow_rate'))

        sfr = (sfr*sfr_unit).to('m**3*s**-1').magnitude
        period = (period*period_unit).to('s').magnitude
        sample_volume = np.multiply(sfr, period)

        svmd = md({"sample_volume": \
                   sample_volume, \
                      "date_time": tvars["date_time"],
                      "Time": tvars["Time"]},
                    unit_spec="default")
        self.do = svmd
        return self.do

    def __repr__(self):
        return "FRSampleVolume"
