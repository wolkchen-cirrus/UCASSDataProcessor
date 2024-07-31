from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
import pandas as pd
import numpy as np
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md


# Redefining print function with timestamp
print = newprint()


def get_arsp_mask(airspeed, limit):
    arsp_store = np.matrix(np.zeros([airspeed.shape[0], 1]))
    for i in range(airspeed.shape[0]):
        if airspeed[i] <= limit:
            arsp_store[i] = 1
        else:
            pass
    return arsp_store


class AirspeedMask(Proc):

    def setup(self):
        self.ivars = ['Airspeed', 'airspeed_lim']
        self.ovars = ['airspeed_mask']
        self.unit_spec = {'airspeed_mask': 'number'}

    def proc(self):
        data = self.get_ivars(dimless=True)
        mask = get_arsp_mask(data['Airspeed'], data['airspeed_lim'])
        timevars = self.get_timevars()
        arspmd = md({"airspeed_mask": mask} | timevars, unit_spec='default')
        self.do = arspmd
        return self.do

    def __repr__(self):
        return "AirspeedMask"




