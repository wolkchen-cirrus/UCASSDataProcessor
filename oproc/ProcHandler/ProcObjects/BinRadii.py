from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from ... import ConfigHandler as ch
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md

import numpy as np
import pandas as pd


# Redefining print function with timestamp
print = newprint()


def interp_linear(lookup: pd.DataFrame, data: pd.DataFrame, x: str, v: str):
    data = pd.merge(lookup, data, left_on=x,\
                      right_on=x, how='outer').interpolate()
    data = pd.merge(lookup, data, left_on=x,\
                      right_on=x, how='left')[v]
    return data.to_list()


class BinRadii(Proc):

    def setup(self):
        self.ivars = ['bcs_sca', 'mat_rad', 'mat_scs', 'bbs_sca']
        self.ovars = ['bvs', 'bc_rads', 'bb_rads']
        self.unit_spec = {'bvs': 'um**3', 'bc_rads': 'um', 'bb_rads': 'um'}

    def proc(self):
        data = self.get_ivars(dimless=True)
        matdat = pd.DataFrame({k: data[k] for k in ['mat_rad', 'mat_scs']})

        lookup = pd.DataFrame({"mat_scs": data['bcs_sca']})
        bc_rads = interp_linear(lookup, matdat, 'mat_scs', 'mat_rad')

        lookup = pd.DataFrame({"mat_scs": data['bbs_sca']})
        bb_rads = interp_linear(lookup, matdat, 'mat_scs', 'mat_rad')

        bvs = [4/3*np.pi*x**3 for x in bc_rads]
        self.do = {'bvs': bvs, 'bc_rads': bc_rads, 'bb_rads': bb_rads}
        return self.do

    def __repr__(self):
        return "BinRadii"
