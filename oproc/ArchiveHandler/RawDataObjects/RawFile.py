from .iss import iss
from .. import Utilities as utils
from ... import newprint
from .. import ImportLib as im
from .. import MavLib as mav
from ..GenericDataObjects.MatrixDict import MatrixDict as md

import os.path
import pandas as pd
import warnings
import numpy as np

# Redefining print function with timestamp
print = newprint()


class RawFile(object):
    """
    This is essentially a wrapper for a raw data file(s). The file(s) is/are
    read only.
    """

    def __init__(self, iss: iss, nc: bool = False):
        self.__fn = None
        self.__nc = nc
        self.iss = iss
        self.fn: list = [utils.get_log_path
                         (f, self.iss.dflags[f]['type']) if not
                         os.path.isdir(utils.get_log_path(f, self.iss.dflags[f]
                                                          ['type'])) else np.nan
                         for f in list(iss.dflags.keys())]

        print(f'File check returns {bool(self)}')

    def __bool__(self):
        return self.__file_check()

    def __repr__(self):
        return f'{self.fn} raw files'

    def __enter__(self):
        self.__f = [open(f, 'r') if isinstance(f, str) else np.nan for f in self.fn]
        return self

    def __exit__(self, type, val, trace):
        for f in self.__f:
            try:
                f.close()
            except AttributeError:
                pass

    def read(self) -> dict[md]:
        if not self.__file_check():
            raise RuntimeError(f"File check is {bool(self)}")
        iss = self.iss.dflags
        data = {}
        for k, f in zip(iss, self.__f):
            try:
                timezone = iss[k]["timezone"]
            except KeyError:
                timezone = None
            if isinstance(k, str):
                print("Processing file %s" % k)
                print(iss[k]['data'])
            else:
                print("No file of type %s for measurement" % iss[k]['type'])
                continue
            lt = utils.infer_log_type(k)
            if lt == '.json':
                messages = dict([(k, [i[0] if isinstance(i, list)
                                      else i for i in v])
                                 for k, v in iss[k]['data'].items()])
                data[k] = mav.read_json_log(k, messages)
            elif lt == '.log':
                warnings.warn('Attempting to parse FC .log file')
                messages = dict([(k, [i[0] if isinstance(i, list)
                                      else i for i in v])
                                 for k, v in iss[k]['data'].items()])
                data[k] = mav.read_mavlink_log(k, messages)
            elif lt == '.csv':
                proc = {}
                tp = iss[k]['type']
                for key in ['cols', 'srow', 'procrows']:
                    try:
                        proc[key] = iss[k]['data'][key]
                    except KeyError:
                        proc[key] = None
                data[k] = self.__proc_cols(k, proc['cols'],
                                           proc['srow'], tp, timezone)\
                    | self.__proc_rows(f, proc['procrows'])
            else:
                raise ValueError("Invalid log file extension %s" % lt)
            print(f'imported data containing {data[k].keys()}')

            if self.__nc == False:
                data[k] = md(data[k] | {"date_time": im.fn_datetime(k)},
                             unit_spec=self.iss.uspec)
            elif self.__nc == True:
                pass
            else:
                print(f'value for nc value is invalid ({self.__nc})')
        return data

    def __file_check(self) -> bool:
        """Returns false if file is invalid or does not exist"""
        if not list(filter(None, [os.path.exists(f)
                                  if isinstance(f, str) else None
                                  for f in self.fn])):
            return False
        try:
            t = [False if f.closed else True for f in self.__f]
            if 0 in t:
                return False
        except AttributeError:
            pass
        return True

    def __proc_cols(self, fn, cols, s_row, t, tz):
        if not cols:
            return {}
        fn = utils.get_log_path(fn, t)
        if not s_row:
            s_row = 0
        else:
            s_row = int(s_row)
        print(f'cols are{cols}')
        names = [x[0] if isinstance(x, list) else x for x in cols if x != '']
        print(names)
        #use_cols = [i for i, x in enumerate(names) if x]
        use_cols = [i for i, x in enumerate(cols) if x]
        print(use_cols)
        d_out = pd.read_csv(fn, header=s_row, names=names, usecols=use_cols)
        if not tz:
            print("Assuming UTC")
        else:
            tz = int(tz)
        if self.__nc == True:
            df_dict = im.df_to_dict(d_out, inc_index=False)
        else:
            d_out['Time'] = [im.infer_datetime(fn, x, tz) \
                             for x in d_out['Time']]
            d_out = d_out.set_index(d_out['Time'])
            d_out = d_out.drop('Time', axis=1)
            df_dict = im.df_to_dict(d_out)
        return df_dict

    @staticmethod
    def __proc_rows(f, proc_rows):
        if not proc_rows:
            return {}
        d_out = {}
        d = f.readlines()
        for rn in proc_rows:
            print(f'row is {rn}')
            print(proc_rows[rn])
            try:
                pr = int(proc_rows[rn]["row"])
                d_out[rn] = d[pr].split(',')[int(proc_rows[rn]["cols"]
                                                 .split(':')[0]):
                                             int(proc_rows[rn]["cols"]
                                                 .split(':')[-1])]
            except TypeError:
                pr = int(proc_rows[rn][0]["row"])
                d_out[rn] = d[pr].split(',')[int(proc_rows[rn][0]["cols"]
                                                 .split(':')[0]):
                                             int(proc_rows[rn][0]["cols"]
                                                 .split(':')[-1])]
                d_out[rn] = [float(x) for x in d_out[rn]]
        print(d_out)
        return d_out

    @property
    def fn(self) -> list:
        return self.__fn

    @fn.setter
    def fn(self, val: list):
        for fn in val:
            print(fn)
            if isinstance(fn, str):
                pass
            elif np.isnan(fn):
                continue
            elif not os.path.isabs(fn):
                raise ValueError("must be abs path")
        self.__fn = val
