from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md


# Redefining print function with timestamp
print = newprint()


class SampleVolume(Proc):

    def setup(self):
        self.ivars = ['Period', 'Airspeed', 'sample_area']
        self.ovars = ['number_conc']
        self.unit_spec = {'number_conc': 'm**-3'}

    def proc(self):
        data = self.di.__get__()
        counts = pl.get_all_suffix('C#', data)
        sv = data["sample_volume"]
        self.do = 
        return self.do

    def __repr__(self):
        return "NConc"
