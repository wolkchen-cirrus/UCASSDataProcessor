from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
import numpy as np


# Redefining print function with timestamp
print = newprint()


class SampleVolume(Proc):

    def setup(self):
        self.ivars = ['Period', 'Airspeed', 'SA']
        self.ovars = ['sample_volume']
        self.unit_spec = {'sample_volume': 'm**3'}

    def proc(self):
        data = self.di.__get__()
        sa = data["SA"]
        period = data["Period"]
        arsp = data["Airspeed"]
        svmd = md({"sample_volume": \
                   np.multiply(period.__get__(), arsp.__get__())*sa, \
                      "date_time": data["date_time"],
                      "Time": data["Time"]},
                    unit_spec="default")
        self.do = svmd
        return self.do

    def __repr__(self):
        return "SampleVolume"
