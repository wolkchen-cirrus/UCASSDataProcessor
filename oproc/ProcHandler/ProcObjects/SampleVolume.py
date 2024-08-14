from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
import numpy as np
import os


# Redefining print function with timestamp
print = newprint()


class SampleVolume(Proc):

    def setup(self):
        self.ivars = ['Period', 'Airspeed', 'corrected_airspeed', 'SA']
        self.ovars = ['sample_volume']
        self.unit_spec = {'sample_volume': 'm**3'}

    def proc(self):
        data = self.get_ivars()
        tvars = self.get_timevars()
        sa = data["SA"]
        period = data["Period"]
        try:
            arsp_type = os.environ['AIRSPEED_TYPE']
            if arsp_type == 'corrected':
                arsp = data["corrected_airspeed"]
            elif arsp_type == 'normal':
                arsp = data["Airspeed"]
            else:
                raise ValueError(f'invalid value of AIRSPEED_TYPE env var: \
                                 {arsp_arsp_type}')
        except KeyError:
            print('env var AIRSPEED_TYPE is not set, assuming normal \
                  airspeed')
            arsp = data["Airspeed"]
        svmd = md({"sample_volume": \
                   np.multiply(period, arsp)*sa, \
                      "date_time": tvars["date_time"],
                      "Time": tvars["Time"]},
                    unit_spec="default")
        self.do = svmd
        return self.do

    def __repr__(self):
        return "SampleVolume"
