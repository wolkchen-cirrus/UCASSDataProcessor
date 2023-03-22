from .__Proc import Proc
from datetime import datetime as dt


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({dt.now()})', *args, **kwargs)


print = timestamped_print


class Calibrate(Proc):

    def __proc(self, **kwargs):
        if "bbs_um" in self.di.__dict__():
            print("Already calibrated, nothing to do")
            return self.di
        return
