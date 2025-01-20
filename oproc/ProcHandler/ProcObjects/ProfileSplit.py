from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
from scipy.signal import find_peaks
import numpy as np
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md


# Redefining print function with timestamp
print = newprint()


def split_by_pressure(press_hpa: np.matrix, press_lim,\
                      prom: int = 15, exp_range: tuple = (1, 5)) -> np.matrix:
    """This is the findpeaks function from the old software with a few mods"""
    norm_press_hpa = press_hpa.astype(float) - float(press_hpa[0,0])
    norm_press_hpa = np.squeeze(np.asarray(norm_press_hpa))

    counter = 0
    while True:
        p_peaks, _ = find_peaks(norm_press_hpa, \
                                prominence=prom, distance=10)
        n_peaks, _ = find_peaks(norm_press_hpa * -1, \
                                prominence=prom, distance=10)
        num_peaks = int(np.shape(n_peaks)[0])
        num_prof = num_peaks*2

        head_cursor = 0
        for pressure in norm_press_hpa:
            if abs(pressure) >= abs(press_lim):
                p_peaks = np.hstack((np.array([head_cursor]), p_peaks))
                break
            else:
                head_cursor += 1
        tail_cursor = int(np.shape(norm_press_hpa)[0])
        for pressure in np.flip(np.squeeze(norm_press_hpa)):
            if abs(pressure) >= abs(press_lim):
                p_peaks = np.hstack((p_peaks, np.array([tail_cursor])))
                break
            else:
                tail_cursor -= 1
        if not (int(np.shape(n_peaks)[0]) + 1 == int(np.shape(p_peaks)[0])) or\
           (num_prof > exp_range[1]):
            prom += 10
            counter += 1
            if (counter > 200) or (num_prof < exp_range[0]):
                print(num_prof)
                input()
                raise ValueError("Problem detecting peaks")
        else:
            break

    print(f"{num_prof} profiles detected")
    if num_prof <= 1:
        raise ValueError("Not enought profiles detected, revise inputs")

    profile_store = np.matrix(np.zeros((np.shape(press_hpa)[0], num_prof)))
    j = 0
    for i in list(range(num_peaks)):
        profile_store[p_peaks[i]:n_peaks[i], j] = 1
        j += 1
        profile_store[n_peaks[i]:p_peaks[i+1], j] = 1
        j += 1

    print(np.shape(profile_store))
    return profile_store


class ProfileSplit(Proc):

    def setup(self):
        self.ivars = ['Press']
        self.ovars = ['PMask#']
        self.unit_spec = {'PMask#': 'number'}

    def proc(self):
        data = self.di.__get__()
        press = data["Press"].__get__()
        pmsk = split_by_pressure(press, -10)
        pmsk = {"PMask"+str((i+1)): pmsk[:, i] \
                for i in range(np.shape(pmsk)[1])}
        print(pmsk)
        psmd = md(pmsk | {"Time": data["Time"],
                   "date_time":data["date_time"]}, unit_spec="default")
        self.do = psmd
        return self.do

    def __repr__(self):
        return "ProfileSplit"
