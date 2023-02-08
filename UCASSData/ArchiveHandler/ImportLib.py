"""
Contains functions for importing raw data into the software. 
"""
import importlib
import os.path
from UCASSData import ConfigHandler as ch
import dateutil.parser as dup
from ..ArchiveHandler import MavLib as mav
from ..ArchiveHandler import Utilities as utils
import importlib as imp
import pandas as pd
import warnings
import numpy as np


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


def sync_and_resample(df_list: list, period_str: str) -> pd.DataFrame:
    """
    A function to synchronise a number of pandas data frames, then resample
    with a given time period.

    :param df_list: A list of pandas data frames to be synchronised.
    :type df_list: list
    :param period_str: The time period for resampling e.g. '0.1S'.
    :type period_str: str

    :return: The synchronised and resampled data frame.
    :rtype: pd.DataFrame
    """
    df = df_list[0]
    for i in range(len(df_list)-1):
        df = pd.merge(df, df_list[i+1], how='outer', left_index=True,
                      right_index=True)
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
    dt = dup.parse(dts)
    if (sdt-dt).total_seconds()/(60.0**2*24) > 1:
        dt = dup.parse(dts, dayfirst=True)
        if (sdt - dt).total_seconds() / (60.0 ** 2 * 24) > 1:
            raise ValueError("Could not infer dt format, revise input")
    return dt


def fn_datetime(fn):
    """
    Retrieves datetime from filename in standard format

    :param fn: filename (abs path ok)
    :type fn: list/str

    :return: datetime of file as list or dt if dt specified
    :rtype: dt.datetime
    """
    fn = to_list(fn)
    dt = []
    for f in fn:
        dt.append(pd.to_datetime('_'.join([f.split('_')[-3], f.split('_')[-2]])
                                 , format='%Y%m%d_%H%M%S%f'))
    if len(dt) == 1:
        return dt[0]
    else:
        return dt


def to_string(s) -> str:
    """
    Convert object to string

    :param s: input string

    :return: converted object
    :rtype: str
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
    :rtype: list
    """
    if isinstance(s, list):
        return s
    else:
        return [s]


def df_to_matrix_dict(df: pd.DataFrame) -> dict:
    """
    Turns a pandas dataframe into a dict of matrix columns for input into
    import structs

    :param df: dataframe
    :type df: pd.DataFrame

    :returns: dictionary with standard type keys
    :rtype: dict
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
    :type iss: dict
    :param dkey: key where the col data is, default 'cols'
    :type dkey: str
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
        if flag not in vf:
            ch.getconf('valid_flags')
            raise ReferenceError('Data flag \'%s\' is not valid' % flag)


def proc_iss(iss: dict) -> dict:
    """
    Main proc system for import struct specification; transfers data from files
    into a dictionary.

    :param iss: import struct specification
    :type iss: dict

    :returns: dictionary of data in matrices
    :rtype: dict
    """
    check_valid_cols(iss)

    def _proc_cols(fn, cols, s_row, t):
        if not cols:
            return {}
        fn = utils.get_log_path(fn, t)
        if 'Time' not in cols:
            raise ValueError('File does not have a datetime index')
        if not s_row:
            s_row = 0
        else:
            s_row = int(s_row)
        d_out = pd.read_csv(fn, header=s_row, names=cols)
        d_out['Time'] = [infer_datetime(fn, x) for x in d_out['Time']]
        d_out = d_out.set_index(d_out['Time'])
        d_out = d_out.drop('Time', axis=1)
        return df_to_matrix_dict(d_out)

    def _proc_row(fn, proc_rows, t):
        if not proc_rows:
            return {}
        fn = utils.get_log_path(fn, t)
        d_out = {}
        with open(fn) as f:
            d = f.readlines()
            for rn in proc_rows:
                pr = int(proc_rows[rn])
                d_out[rn] = d[pr]
        return d_out

    data = {}
    for k in iss:
        if k is np.nan:
            print("No file of type %s for measurement" % iss[k]['type'])
            continue
        lt = utils.infer_log_type(k)
        if lt == '.json':
            data[k] = mav.read_json_log(k, iss[k]['data'])
        elif lt == '.log':
            warnings.warn('Attempting to parse FC .log file, go make a coffee '
                          'this will take a while :D')
            data[k] = mav.read_mavlink_log(k, iss[k]['data'])
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

    return data


def populate_data_objects(data: dict) -> list:
    """
    Populates the objects in UCASSData.ArchiveHandler.DataObjects with raw data

    :param data: the data for one instance
    :type data: dict

    :return: list of "DataObject" objects
    :rtype: list
    """

    tom = ch.getval("type_object_map")
    tp = data['type']
    module = importlib.import_module('UCASSData.ArchiveHandler.DataObjects.' +
                                     tom[tp])
    data_obj = getattr(module, tom[tp])


    return []
