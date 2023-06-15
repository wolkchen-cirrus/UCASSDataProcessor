from .__Proc import Proc
from datetime import datetime as dt
from ..ProcHandler import ProcLib as pl


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({dt.now()})', *args, **kwargs)


print = timestamped_print


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
        cof = data["cali_coeffs"]
        mie_curve = pl.get_material_data(mat)

    def __repr__(self):
        return "Calibrate"
