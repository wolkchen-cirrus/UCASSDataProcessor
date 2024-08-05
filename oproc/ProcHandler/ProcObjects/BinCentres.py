from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md

import numpy as np


# Redefining print function with timestamp
print = newprint()


class BinCentres(Proc):

    def setup(self):
        self.ivars = ['bbs_sca']
        self.ovars = ['bcs_sca']
        self.unit_spec = {'bcs_sca': 'um**2'}

    def proc(self):
        data = self.get_ivars()
        bcs = []
        for i, v in enumerate(data['bbs_sca'][:-1]):
            bcs.append(np.sqrt(v*data['bbs_sca'][i+1]))
        self.do = {'bcs_sca': bcs}
        return self.do

    def __repr__(self):
        return "BinCentres"
