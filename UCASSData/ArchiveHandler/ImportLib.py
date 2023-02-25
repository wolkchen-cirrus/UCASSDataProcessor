"""
Contains functions for importing raw data into the software.
"""
import os.path
from .. import ConfigHandler as ch
from collections.abc import MutableMapping
import dateutil.parser as dup
import datetime as dt
import pandas as pd
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
        if '_CalData_' in fn:
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
    for i in range(len(df_list) - 1):
        df = pd.merge(df, df_list[i + 1], how='outer', left_index=True,
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
        for i in range(len(datetimes) - 1):
            delta.append((date - datetimes[i + 1]).total_seconds() / 60.0)
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
    if (sdt - dti).total_seconds() / (60.0 ** 2 * 24) > 1:
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
