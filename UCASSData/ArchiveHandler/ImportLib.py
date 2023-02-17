"""
Contains functions for importing raw data into the software. 
"""
import os.path
from UCASSData import ConfigHandler as ch
from collections.abc import MutableMapping
import dateutil.parser as dup
import datetime as dt
import pandas as pd
import warnings
import numpy as np
from ..ArchiveHandler import MavLib as mav
from ..ArchiveHandler import Utilities as utils
from UCASSData.ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict


def get_ucass_calibration(serial_number: str) -> tuple:
    """
    A function to retrieve the calibration coefficients of a UCASS unit, given
    its serial number.

    :param serial_number: The serial number of the UCASS unit.
    :type serial_number: str

    :return: gain (float) and sl (float), the calibration coefficients.
    :rtype: tuple
    """
    cali_path = os.path.join(ch.getval('ucass_calibration_path'),
                             serial_number)
    cal_file = None
    for fn in os.listdir(cali_path):
        if 'CalData' in fn:
            cal_file = fn
            break
    if not cal_file:
        raise FileNotFoundError("Calibration file does not exist")
    cali_path = os.path.join(cali_path, cal_file)
    with open(cali_path) as cf:
        data = cf.readlines()
    gain = None
    sl = None
    for line in data:
        if 'Gain' in line:
            gain = float(line.split('=')[-1])
        elif 'SL' in line:
            sl = float(line.split('=')[-1])
    if (not gain) or (not sl):
        raise AttributeError("Either gain or sl not found in file")
    else:
        return gain, sl


def sync_and_resample(df_list: list, period_str: str,
                      keep_one: bool = False) -> pd.DataFrame:
    """
    A function to synchronise a number of pandas data frames, then resample
    with a given time period.

    :param df_list: A list of pandas data frames to be synchronised
    :param period_str: The time period for resampling e.g. '0.1S'
    :param keep_one: if two col names are the same, keep one (True) or both

    :return: The synchronised and resampled data frame.
    """
    df = df_list[0]
    if keep_one is False:
        suffixes = ('_x', '_y')
    else:
        suffixes = (None, '_%%SUFFIX%%')
    for i in range(len(df_list)-1):
        df = pd.merge(df, df_list[i+1], how='outer', left_index=True,
                      right_index=True, suffixes=suffixes)
    df = df[df.columns.drop(list(df.filter(regex='%%SUFFIX%%')))]
    return df.dropna(how='all', axis=0).dropna(how='all', axis=1)\
        .resample(period_str).mean().bfill()


def check_datetime_overlap(datetimes: list, tol_mins: int = 30):
    """
    Checks if any datetime difference exceeds a specified tolerance

    :param datetimes: List of datetimes to be checked
    :type datetimes: list
    :param tol_mins: Max difference between datetimes in minutes
    :type tol_mins: int

    :raise ValueError: If the difference between any two datetimes specified is
    larger than the tolerance
    """
    datetimes = to_list(datetimes)
    delta = []
    for date in datetimes:
        for i in range(len(datetimes)-1):
            delta.append((date - datetimes[i+1]).total_seconds()/60.0)
    if all(x > tol_mins for x in delta):
        raise ValueError("Time delta exceeds threshold (%i)" % tol_mins)
    else:
        return


def infer_datetime(fn: str, dts: str):
    """
    Attempts to infer datetime string format using filename as anchor; pretty
    unstable tbh, try not to use this if possible

    :param fn: filename
    :type fn: str
    :param dts: datetime string in rando format
    :type dts: string

    :returns: datetime
    :rtype: dt.datetime
    """
    sdt = fn_datetime(fn)
    dti = dup.parse(dts)
    if (sdt-dti).total_seconds()/(60.0**2*24) > 1:
        dti = dup.parse(dts, dayfirst=True)
        if (sdt - dti).total_seconds() / (60.0 ** 2 * 24) > 1:
            raise ValueError("Could not infer dt format, revise input")
    return dti


def fn_datetime(fn: list or str) -> dt.datetime or pd.Timestamp:
    """
    Retrieves datetime from filename in standard format

    :param fn: filename (abs path ok)

    :return: datetime of file as list or dt if dt specified
    """
    fn = to_list(fn)
    dti = []
    for f in fn:
        dti.append(pd.to_datetime('_'.join([f.split('_')[-3],
                                            f.split('_')[-2]]),
                                  format='%Y%m%d_%H%M%S%f'))
    if len(dti) == 1:
        return dti[0]
    else:
        return dti


def to_string(s) -> str:
    """
    Convert object to string

    :param s: input string

    :return: converted object
    """
    if isinstance(s, str):
        return s
    else:
        return s.decode(errors="backslashreplace")


def to_list(s) -> list:
    """
    Converts to list for function inputs

    :param s: string or list

    :return: ensured list
    """
    if isinstance(s, list):
        return s
    else:
        return [s]


def df_to_dict(df: pd.DataFrame) -> dict:
    """
    Turns a pandas dataframe into a dict of matrix columns for input into
    import structs

    :param df: dataframe

    :returns: dictionary with standard type keys
    """
    md = df.to_dict(orient='list')
    for key in md:
        md[key] = np.matrix(md[key]).T
    md['Time'] = df.index
    return md


def check_valid_cols(iss: dict, dkey: str = 'cols'):
    """
    Checks the cols in a given struct spec are valid. Valid cols are specified
    in the 'valid_flags' config variable

    :param iss: import struct spec
    :param dkey: key where the col data is, default 'cols'

    :raise ReferenceError: if one or more flags are not valid
    """
    fields = []

    def _finditem(obj, key):
        if key in obj:
            fields.append(obj[key])
        for k, v in obj.items():
            if isinstance(v, dict):
                item = _finditem(v, key)
                if item is not None:
                    return item

    _finditem(iss, dkey)
    fields = [i for s in fields for i in s if not isinstance(s, str)]
    vf = [x['name'] for x in ch.getval('valid_flags')]
    for flag in fields:
        if isinstance(flag, list):
            flag = flag[0]
        if flag not in vf:
            ch.getconf('valid_flags')
            raise ReferenceError('Data flag \'%s\' is not valid' % flag)
    print("All flags valid")


def proc_iss(iss: dict) -> dict:
    """
    Main proc system for import struct specification; transfers data from files
    into a dictionary.

    :param iss: import struct specification

    :returns: dictionary of data in matrices
    """
    print("Processing data specified by import struct .json")
    check_valid_cols(iss)
    unit_spec = make_unit_spec(iss)

    def _proc_cols(fn, cols, s_row, t):
        if not cols:
            return {}
        fn = utils.get_log_path(fn, t)
        if not s_row:
            s_row = 0
        else:
            s_row = int(s_row)
        names = [x[0] if isinstance(x, list) else x for x in cols]
        d_out = pd.read_csv(fn, header=s_row, names=names)
        d_out['Time'] = [infer_datetime(fn, x) for x in d_out['Time']]
        d_out = d_out.set_index(d_out['Time'])
        d_out = d_out.drop('Time', axis=1)
        df_dict = df_to_dict(d_out)
        return df_dict

    def _proc_row(fn, proc_rows, t):
        if not proc_rows:
            return {}
        fn = utils.get_log_path(fn, t)
        d_out = {}
        with open(fn) as f:
            d = f.readlines()
            for rn in proc_rows:
                pr = int(proc_rows[rn])
                d_out[rn] = d[pr].split(',')
        return d_out

    data = {}
    for k in iss:
        if k is np.nan:
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
            warnings.warn('Attempting to parse FC .log file, go make a coffee '
                          'this will take a while :D')
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
            data[k] = _proc_cols(k, proc['cols'], proc['srow'], tp) | \
                _proc_row(k, proc['procrows'], tp) | {'type': tp}
        else:
            raise ValueError("Invalid log file extension %s" % lt)
        data[k] = MatrixDict(data[k] | {"date_time": fn_datetime(k)},
                             unit_spec=unit_spec)

    return data


def make_unit_spec(iss: dict) -> dict:

    fields = []

    def _finditem(obj, key):
        if key in obj:
            fields.append(obj[key])
        for k, v in obj.items():
            if isinstance(v, dict):
                item = _finditem(v, key)
                if item is not None:
                    return item

    _finditem(iss, 'data')
    fields = [s[i] for s in fields for i in s]
    c = [tuple(i) for s in fields for i in s if isinstance(i, list)]
    r = [(j, k[1]) for j, k in [i for s in [list(x.items()) for x in fields
                                            if isinstance(x, dict)]
                                for i in s if isinstance(i[1], list)]]
    return dict(c+r)


def serial_number_from_fn(fn: str) -> str:
    fn = os.path.split(fn)[1]
    fn = fn.split("_")
    sn = [x for x in fn if "UCASS" in x]
    if len(sn) != 1:
        raise LookupError
    else:
        return sn[0]


def flatten_dict(d: MutableMapping) -> MutableMapping:
    """
    flattens dictionary recursively

    :param d: dict to flatten
    """
    items = []
    for k, v in d.items():
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v).items())
        else:
            items.append((k, v))
    return dict(items)
