"""
Contains functions for importing raw data into the software. 
"""

import os.path
from UCASSData import ConfigHandler as ch
import pandas as pd


def get_ucass_calibration(serial_number):
    """
    A function to retrieve the calibration coefficients of a UCASS unit, given its serial number.

    :param serial_number: The serial number of the UCASS unit.
    :type serial_number: str

    :return: gain (float) and sl (float), the calibration coefficients.
    :rtype: tuple
    """
    cali_path = os.path.join(ch.read_config_key('ucass_calibration_path'), serial_number)
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


def sync_and_resample(df_list, period_str):
    """
    A function to synchronise a number of pandas data frames, then resample with a given time period.

    :param df_list: A list of pandas data frames to be synchronised.
    :type df_list: list
    :param period_str: The time period for resampling, specified as a string, e.g. '0.1S'.
    :type period_str: str

    :return: The synchronised and resampled data frame.
    :rtype: pd.DataFrame
    """
    df = df_list[0]
    for i in range(len(df_list)-1):
        df = df.join(df_list[i+1])
    return df.dropna(how='all', axis=0).dropna(how='all', axis=1).resample(period_str).mean().bfill()


def check_datetime_overlap(datetimes, tol_mins=30):
    """
    Checks if any datetime difference exceeds a specified tolerance

    :param datetimes: List of datetimes to be checked
    :type datetimes: list
    :param tol_mins: Max difference between datetimes in minutes
    :type tol_mins: int

    :raise ValueError: If the difference between any two datetimes specified is larger than the tolerance
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


def fn_datetime(fn):
    """
    Retrieves datetime from filename in standard format

    :param fn: filename (abs path ok)
    :type fn: str

    :return: datetime of file
    :rtype: dt.datetime
    """
    return pd.to_datetime('_'.join([fn.split('_')[-3], fn.split('_')[-2]]), format='%Y%m%d_%H%M%S%f')


def to_string(s):
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


def to_list(s):
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
