import os.path
import pandas as pd
from .iss import iss
from .. import Utilities as utils
from .. import ImportLib as im
from .. import MavLib as mav
from numpy import nan
from ..GenericDataObjects.MatrixDict import MatrixDict as md
import warnings
from datetime import datetime


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({datetime.now()})', *args, **kwargs)


print = timestamped_print


class RawFile(object):
    """
    This is essentially a wrapper for a raw data file(s). The file(s) is/are
    read only.
    """

    def __init__(self, iss: iss):
        self.__fn = None

        self.iss = iss
        self.fn: list = [utils.get_log_path
                         (f, self.iss.dflags[f]['type']) if not
                         os.path.isdir(utils.get_log_path(f, self.iss.dflags[f]
                                                          ['type'])) else nan
                         for f in list(iss.dflags.keys())]

        print(f'File check returns {bool(self)}')

    def __bool__(self):
        return self.__file_check()

    def __repr__(self):
        return f'{self.fn} raw files'

    def __enter__(self):
        self.__f = [open(f, 'r') if f is not nan else nan for f in self.fn]
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
            if k is nan:
                print("No file of type %s for measurement" % iss[k]['type'])
                continue
            else:
                print("Processing file %s" % k)
                print(iss[k]['data'])
            lt = utils.infer_log_type(k)
            if lt == '.json':
                messages = dict([(k, [i[0] if isinstance(i, list)
                                      else i for i in v])
                                 for k, v in iss[k]['data'].items()])
                data[k] = mav.read_json_log(k, messages)
                data[k]['type'] = iss[k]['type']
            elif lt == '.log':
                warnings.warn('Attempting to parse FC .log file')
                messages = dict([(k, [i[0] if isinstance(i, list)
                                      else i for i in v])
                                 for k, v in iss[k]['data'].items()])
                data[k] = mav.read_mavlink_log(k, messages)
                data[k]['type'] = iss[k]['type']
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
                    | self.__proc_rows(f, proc['procrows'])\
                    | {'type': tp}
            else:
                raise ValueError("Invalid log file extension %s" % lt)
            data[k] = md(data[k] | {"date_time": im.fn_datetime(k)},
                         unit_spec=self.iss.uspec)
        return data

    def __file_check(self) -> bool:
        """Returns false if file is invalid or does not exist"""
        if not list(filter(None, [os.path.exists(f)
                                  if f is not nan else None
                                  for f in self.fn])):
            return False
        try:
            t = [False if f.closed else True for f in self.__f]
            if 0 in t:
                return False
        except AttributeError:
            pass
        return True

    @staticmethod
    def __proc_cols(fn, cols, s_row, t, tz):
        if not cols:
            return {}
        fn = utils.get_log_path(fn, t)
        if not s_row:
            s_row = 0
        else:
            s_row = int(s_row)
        names = [x[0] if isinstance(x, list) else x for x in cols]
        use_cols = [i for i, x in enumerate(names) if x]
        d_out = pd.read_csv(fn, header=s_row, names=names, usecols=use_cols)
        if not tz:
            print("Assuming UTC")
        else:
            tz = int(tz)
        d_out['Time'] = [im.infer_datetime(fn, x, tz) for x in d_out['Time']]
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
            pr = int(proc_rows[rn]["row"])
            d_out[rn] = d[pr].split(',')[int(proc_rows[rn]["cols"]
                                             .split(':')[0]):
                                         int(proc_rows[rn]["cols"]
                                             .split(':')[-1])]
        return d_out

    @property
    def fn(self) -> list:
        return self.__fn

    @fn.setter
    def fn(self, val: list):
        for fn in val:
            if fn is nan:
                continue
            elif not os.path.isabs(fn):
                raise ValueError("must be abs path")
        self.__fn = val
