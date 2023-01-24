"""
Contains functions for importing raw data into the software. 
"""

import os.path
from UCASSData import ConfigHandler as ch
import datetime as dt
import pandas as pd
import numpy as np
import subprocess


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
        df = pd.concat(df.align(df_list[i+1]), axis='columns')
    return df.dropna(how='all', axis=0).dropna(how='all', axis=1).resample(period_str).mean().bfill()


def read_mavlink_log(log_path, message_names):
    """
    A function to read a mavlink log, with specified message and data names, into arrays.

    :param log_path: The path to the mavlink '.log' file
    :type log_path: str
    :param message_names: Specification of message names where message_names['name'] = '[var1, var2, ...]'
    :type message_names: dict

    :return: The synchronised and resampled data frame.
    :rtype: pd.DataFrame
    """
    def _proc_fc_row(fc_row, params):
        time = dt.datetime.strptime(fc_row[:22], '%Y-%m-%d %H:%M:%S.%f')
        output = []
        fc_row = fc_row.split(',')
        for param in params:
            for fc_part in fc_row:
                label = fc_part.split(':')[0]
                if param.replace(' ', '') == label.replace(' ', ''):
                    output.append(float(fc_part.split(':')[-1]))
                    break
        return time, output

    mav_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mavlogdump.py')
    proc = subprocess.Popen(['python', mav_path, "--types", ','.join(message_names.keys()), log_path],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    fd_out = proc.communicate()[0].decode("utf-8")
    fd_out = fd_out.split('\n')
    fd = {}
    for mess in message_names:
        fd_list = []
        for r in fd_out:
            if mess in r:
                fd_list.append(r)
        fd[mess] = fd_list
    
    fc_dict = {}
    for mess in message_names:
        fc_list = []
        fc_time = []
        for row in fd[mess]:
            fc_time_row, proc_row = _proc_fc_row(row, message_names[mess])
            fc_list.append(proc_row)
            fc_time.append(fc_time_row)
        fc_dict[mess] = pd.DataFrame(fc_list, columns=message_names[mess], index=fc_time)

    df = sync_and_resample(list(fc_dict.values()), '0.1S')

    fc_dict = df.to_dict(orient='list')
    for key in fc_dict:
        fc_dict[key] = np.matrix(fc_dict[key]).T
    fc_dict['Time'] = df.index

    return fc_dict


def check_datetime_overlap(datetimes, tol_mins=30):
    """
    Checks if any datetime difference exceeds a specified tolerance

    :param datetimes: List of datetimes to be checked
    :type datetimes: list
    :param tol_mins: Max difference between datetimes in minutes
    :type tol_mins: int

    :raise ValueError: If the difference between any two datetimes specified is larger than the tolerance
    """
    delta = []
    for date in datetimes:
        for i in range(len(datetimes)-1):
            delta.append((date - datetimes[i+1]).total_seconds()/60.0)
    if all(x > tol_mins for x in delta):
        raise ValueError("Time delta exceeds threshold (%i)" % tol_mins)
    else:
        return
