from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from ... import ureg
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
import numpy as np
import os


# Redefining print function with timestamp
print = newprint()


class GetPeriod(Proc):

    def setup(self):
        self.ivars = ['Time']
        self.ovars = ['Period']
        self.unit_spec = {'Period': 's'}

    def proc(self):
        data = self.get_ivars()
        tvars = self.get_timevars()

        time_arr = data['Time']
        dt = []
        for ti, tip in zip(time_arr[1:], time_arr[:-1]):
            dt.append((tip-ti).total_seconds())
        dt.append(dt[-1])
        dt = np.matrix(dt).T

        pmd = md(
                    {
                        "Period": dt,
                        "date_time": tvars["date_time"],
                        "Time": tvars["Time"]
                    },
                    unit_spec="default"
        )
        self.do = pmd
        return self.do

    def __repr__(self):
        return "FRSampleVolume"
