from .__Proc import Proc
from ... import newprint


# Redefining print function with timestamp
print = newprint()


class Calibrate(Proc):

    def __proc(self, **kwargs):
        data = self.di.__dict__()
        if "bbs_sca" in data:
            print("Already calibrated, nothing to do")
            return self.di
        else:
            data = data["bbs"]
        cof = data["cali_coeffs"]
        return [(x - cof[1]) / cof[0] for x in data]

    def __repr__(self):
        return "Calibrate"
