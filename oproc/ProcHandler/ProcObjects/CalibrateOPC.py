from .__Proc import Proc
from .. import ProcLib as pl
import numpy as np
from ... import newprint
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md


# Redefining print function with timestamp
print = newprint()


class CalibrateOPC(Proc):

    def setup(self):
        self.ivars = ['bbs', 'cali_coeffs']
        self.ovars = ['bbs_sca']
        self.unit_spec = {'bbs_sca': 'm**2'}

    def proc(self):
        data = self.di.__get__()
        bbs = data['bbs']
        cof = data["cali_coeffs"]
        sca = np.array([(x - cof[0]) / cof[1] for x in bbs])
        sca[sca < 0] = np.nan
        self.do = {"bbs_sca": list(sca)}
        return self.do

    def __repr__(self):
        return "Calibrate"
