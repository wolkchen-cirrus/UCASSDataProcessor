from .__Proc import Proc
from .. import ProcLib as pl
from ... import newprint
import numpy as np
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
import datetime as dt


# Redefining print function with timestamp
print = newprint()


def gs_corrected_asp(pitch, yaw, gs, ws_h, alt, time, wa_deg):
    """airspeed correction based on groundspeed and ground level wind speed"""

    def __dy_dx(y_arr, x_arr):
        out = np.zeros(np.shape(y_arr))
        for x1, x2, y1, y2, i in \
                zip(x_arr[0:-1], x_arr[1:], y_arr[0:-1], y_arr[1:],\
                    [i+1 for i in (range(y_arr.shape[0]-1))]):
            out[i] = (y2 - y1) / (x2 - x1)
        return out

    def __mag(vector):
        return np.sqrt(np.sum(vector**2))

    def __angle(v1, v2):
        a = np.arccos((np.true_divide(np.dot(v1, v2), \
                                      np.multiply(__mag(v1), __mag(v2)))))
        return a


    ws_v_arr = -1 * __dy_dx(alt, time)

    wa_rad = np.deg2rad(wa_deg)
    as_store = np.matrix(np.zeros((np.shape(time)[0], 1)))
    #_vw = np.array([np.sin(wa_rad), np.cos(np.pi - wa_rad), 0])
    _vw = np.array([np.sin(wa_rad), np.pi - np.cos(wa_rad), 0])
    _vw = np.multiply(ws_h, np.divide(_vw, __mag(_vw)))

    for i in list(range(time.shape[0])):
        p = pitch[i]
        y = yaw[i]
        g_ms = gs[i]
        ws_v = ws_v_arr[i]
        p_rad = np.deg2rad(p)
        y_rad = np.deg2rad(y)

        _as_norm = np.array([np.sin(y_rad)*np.cos(p_rad), \
                        np.cos(y_rad)*np.cos(p_rad), np.sin(p_rad)])
        _as_norm = np.divide(_as_norm, __mag(_as_norm))

        _vg = np.array([np.sin(y_rad), np.cos(y_rad), 0])
        _vg = np.multiply(g_ms, np.divide(_vg, __mag(_vg)))
        _vv = np.array([0, 0, ws_v])
        _vr = _vg + _vw + _vv

        aoa = __angle(_as_norm, _vr)
        mag_as = np.multiply(__mag(_vr), np.cos(aoa))

        as_store[i, 0] = mag_as

    return as_store


class AirspeedCorrection(Proc):

    def setup(self):
        self.ivars = ['Pitch', 'Yaw', 'WD', 'Time', 'Spd', 'WS', 'Alt']
        self.ovars = ['corrected_airspeed']
        self.unit_spec = {'corrected_airspeed': 'mps'}

    def proc(self):
        data = self.get_ivars(dimless=True)
        tdat = self.get_timevars()
        epoch = np.datetime64(dt.datetime(1970, 1, 1))
        tstime = np.array([(data['Time'][i] - epoch) / np.timedelta64(1, 's')
                           for i in range(data['Time'].shape[0])])
        asp = gs_corrected_asp(data['Pitch'], data['Yaw'], data['Spd'],\
                               data['WS'][0], data['Alt'], tstime, data['WD'][0])
        asp[asp < 5] = np.nan
        asmd = md({'corrected_airspeed': asp, 'Time': tdat['Time'], \
                    'date_time': tdat['date_time']}, unit_spec='default')
        self.do = asmd
        return self.do

    def __repr__(self):
        return "AirspeedCorrection"



