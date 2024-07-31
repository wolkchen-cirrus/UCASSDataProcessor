from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
import numpy as np
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
import datetime as dt


# Redefining print function with timestamp
print = newprint()


def check_aoa_fixedwing(pitch, yaw, gs, vz, alt, time, wa_deg,\
                          aoa_lim_deg):
    """The aoa masking algorithm presented in Girdwood et al 2022"""

    def __dy_dx(y_arr, x_arr):
        out = np.zeros(np.shape(y_arr))
        for x1, x2, y1, y2, i in \
                zip(x_arr[0:-1], x_arr[1:], y_arr[0:-1], y_arr[1:],\
                    [i+1 for i in (range(y_arr.shape[0]-1))]):
            out[i] = (y2 - y1) / (x2 - x1)
        return out


    def __mag(vector):
        return np.sqrt(np.sum(vector**2))


    ws_v_arr = -1 * __dy_dx(alt, time)

    wa_rad = np.deg2rad(wa_deg)
    aoa_mask = np.matrix(np.zeros((np.shape(vz)[0], 1)))
    aoa_store = np.matrix(np.zeros((np.shape(vz)[0], 1)))
    for i in list(range(vz.shape[0])):
        p = pitch[i]
        y = yaw[i]
        v_ms = vz[i]
        g_ms = gs[i]
        ws_v = ws_v_arr[i]
        p_rad = np.deg2rad(p)
        y_rad = np.deg2rad(y)
        _as = np.array([np.sin(y_rad)*np.cos(p_rad), \
                        np.cos(y_rad)*np.cos(p_rad), np.sin(p_rad)])
        _as = np.multiply(v_ms, np.divide(_as, __mag(_as)))
        _gnd_as = np.multiply(_as, np.cos(p_rad))
        _gnd_as[-1] = 0
        _gs = np.array([np.sin(y_rad), np.cos(y_rad), 0])
        _gs = np.multiply(g_ms, np.divide(_gs, __mag(_gs)))
        _ws_h = np.divide(_gnd_as - _gs, np.deg2rad(180) - np.cos(wa_rad))
        _ws = _ws_h + np.array([0, 0, ws_v])
        _r = _ws + _as
        aoa = np.arccos((np.true_divide(np.dot(_r, _as), \
                                        np.multiply(__mag(_r), __mag(_as)))))
        aoa_store[i, 0] = np.rad2deg(aoa)

        if aoa_store[i, 0] < aoa_lim_deg:
            aoa_mask[i, 0] = 1
        else:
            pass

    return aoa_mask, aoa_store


class AOAMask(Proc):

    def setup(self):
        self.ivars = ['Pitch', 'Yaw', 'WD', 'Airspeed', 'Time', 'Spd', \
                      'AoA_lim', 'Alt']
        self.ovars = ['AOAMask', 'AoA']
        self.unit_spec = {'AOAMask': 'number', 'AoA': 'deg'}

    def proc(self):
        data = self.get_ivars(dimless=True)
        epoch = np.datetime64(dt.datetime(1970, 1, 1))
        tstime = np.array([(data['Time'][i] - epoch) / np.timedelta64(1, 's')
                           for i in range(data['Time'].shape[0])])
        aoa_mask, aoa = check_aoa_fixedwing(data['Pitch'], data['Yaw'],\
                                            data['Spd'], data['Airspeed'],\
                                            data['Alt'], tstime,\
                                            data['WD'],
                                            aoa_lim_deg=data['AoA_lim'])
        aoamd = md({'AOAMask': aoa_mask, 'AoA': aoa, \
                    'Time': self.di.__get__()['Time'], \
                    'date_time': self.di.__get__()['date_time']}, \
                   unit_spec='default')
        self.do = aoamd
        return self.do

    def __repr__(self):
        return "AOAMask"



