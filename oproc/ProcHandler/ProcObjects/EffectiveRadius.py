from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from ... import ureg
import numpy as np
import pandas as pd
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md


# Redefining print function with timestamp
print = newprint()


def eff_rad_row(nc, bc):
    """gets effective radius of single row using the Korolev method"""
    nc = np.matrix(np.asarray(nc).ravel())
    bc = np.matrix(np.asarray(bc).ravel())
    if np.all(np.isnan(nc)) == True:
        return np.matrix([np.nan])
    else:
        ncbc = np.asarray(np.concatenate([nc, bc], axis=0))
        ncbc = np.matrix(ncbc[:, ~np.isnan(ncbc).any(axis=0)])
        nc = np.asarray(ncbc[0, :]).ravel().tolist()
        bc = np.asarray(ncbc[1, :]).ravel().tolist()
        r_eff = np.true_divide(
            np.sum([n*r**3 for n, r in zip(nc, bc)]),
            np.sum([n*r**2 for n, r in zip(nc, bc)])
        )
        return np.matrix([r_eff])

class EffectiveRadius(Proc):

    def setup(self):
        self.ivars = ['C#', 'sample_volume', 'bc_rads']
        self.ovars = ['effective_radius']
        self.unit_spec = {'effective_radius': 'um'}

    def proc(self):
        data = self.get_ivars()
        tdat = self.get_timevars()
        bc_unit = ureg(self.get_input_unit('bc_rads'))
        sv_unit = ureg(self.get_input_unit('sample_volume'))

        counts = np.concatenate([v for k, v in data.items() if 'C' in k],
                                axis=1)
        sv = np.concatenate([(data['sample_volume']*sv_unit).to('m**3')\
                             .magnitude]*counts.shape[1], axis=1)
        nc_binned = np.divide(counts, sv)
        bc = [(x*bc_unit).to('um').magnitude for x in data['bc_rads']]
        e_rad = np.concatenate([eff_rad_row(x, bc) for x in nc_binned ], axis=0)
        count_threshold = 5
        for i, c in enumerate(np.sum(counts, axis=1)):
            if c[0] < count_threshold:
                e_rad[i] = np.nan

        self.do = md({'effective_radius': e_rad} | tdat, unit_spec='default')
        return self.do

    def __repr__(self):
        return "EffectiveRadius"





