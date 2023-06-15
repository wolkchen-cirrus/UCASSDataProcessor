from .__Proc import Proc
from ..ProcHandler import ProcLib as pl
from ... import newprint


# Redefining print function with timestamp
print = newprint()


class Calibrate(Proc):

    def __proc(self, **kwargs):
        try:
            mat = kwargs["material"]
        except KeyError:
            raise ValueError("Material must be assigned to kwargs")
        data = self.di.__dict__()
        if "bbs_sca" in data:
            print("Already calibrated, nothing to do")
            return self.di
        else:
            data = data["bbs"]
        cof = data["cali_coeffs"]
        mie_curve = pl.get_material_data(mat)
        return [(x - cof[1]) / cof[0] for x in data]

    def __repr__(self):
        return "Calibrate"
