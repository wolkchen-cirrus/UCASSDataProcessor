"""
Contains functions for data archive maintenance and searching. Anything to do with raw data maintenance goes here.
"""

import pandas as pd
import datetime as dt
import numpy as np
from UCASSData import ConfigHandler as ch
from ..ArchiveHandler import ImportLib as im
import os.path


def get_log_path(path, t):
    """
    Returns abs path from log filename and type

    :param path: file path, abd or rel
    :type path: str or None
    :param t: data type (Met, FC, UCASS, &c.)
    :type t: str

    :raise TypeError: If the calculated path does not exist

    :return: the abs path
    :rtype: str
    """
    base = ch.getval('base_data_path')
    try:
        if os.path.isabs(path):
            pass
        else:
            path = os.path.join(base, 'Raw', t, os.path.split(path)[-1])
    except TypeError:
        path = os.path.join(base, 'Raw', t)

    if os.path.exists(path):
        return path
    else:
        raise FileNotFoundError('Path does not exist in structure')


def match_raw_files(files, match_types, tol_min=30):
    """
    A function to find matching raw files by datetime, assuming some data for one data instance were in different files.

    :param files: list of files you want to match
    :type files: list or str
    :param match_types: types of files you want to find matches for. Must be a folder name in the "Raw" directory
    :type match_types: list or str
    :param tol_min: tolerance of matches in minutes, default is 30
    :type tol_min: int

    :return: dataframe of matches where the index is the input files, and the columns are the match types
    :rtype: pd.DataFrame
    """
    files = im.to_list(files)
    match_types = im.to_list(match_types)
    base = ch.getval('base_data_path')
    dt0s = pd.to_datetime(['_'.join(os.path.split(x)[-1].split('_')[-3:-1]) for x in files], format='%Y%m%d_%H%M%S%f')
    mf = pd.DataFrame(index=[os.path.split(x)[-1] for x in files])
    for mt in match_types:
        mfn = []
        for dt0, in_file in zip(dt0s, files):
            tm = os.listdir(os.path.join(base, 'Raw', mt))
            delta_dt = pd.to_datetime(['_'.join(x.split('_')[-3:-1]) for x in tm], format='%Y%m%d_%H%M%S%f')
            delta_dt = abs((delta_dt-dt0).total_seconds()/60.0)
            if min(delta_dt) > tol_min:
                mfn.append(np.nan)
            else:
                mfn.append(tm[list(delta_dt).index(min(delta_dt))])
        mf[mt] = mfn
    return mf


def make_dir_structure():
    """
    A function to create the data storage structure required for the software. If directories already exist, the
    function skips them. The config json is used to find the base path.
    """
    base = ch.getval('base_data_path')
    paths = [base, os.path.join(base, 'Raw'), os.path.join(base, 'Processed'),
             os.path.join(base, 'Raw', 'FC'), os.path.join(base, 'Raw', 'Met'), os.path.join(base, 'Raw', 'Misc'),
             os.path.join(base, 'Raw', 'Rejected'), os.path.join(base, 'Raw', 'UCASS'),
             os.path.join(base, 'Raw', 'FC Proc')]
    for path in paths:
        try:
            os.makedirs(path)
        except FileExistsError:
            print('Directory \"%s\" already exists; skipping directory' % path)


def csv_log_timedelta(filepath, hours, time_format_str, time_header, names=None,
                      change_fn=False, header=0, minutes=0):
    """
    A function to add or subtract a number of hours from a log file

    :param filepath: Absolute file path
    :type filepath: str
    :param hours: Number of hours to add or subtract
    :type hours: int
    :param minutes: Number of hours to add or subtract
    :type minutes: int
    :param time_format_str: Input datetime format string
    :type time_format_str: str
    :param time_header: Which header specified in 'names' is the time column
    :type time_header: str
    :param names: Column names for data frame
    :type names: list
    :param change_fn: Change filename label?
    :type change_fn: bool
    :param header: Which row are the headers on, 0 if no headers
    :type header: int

    :returns: The old and new (if changed) filepaths as a tuple
    :rtype: tuple
    """
    # Change time in file column
    df = pd.read_csv(filepath, delimiter=',', header=header, names=names).dropna()
    if header >= 1:
        with open(filepath) as f:
            meta_data = f.readlines()[:header]
    try:
        df[time_header] = pd.to_datetime(df[time_header], format=time_format_str) \
                          + dt.timedelta(hours=hours, minutes=minutes)
    except ValueError:
        df[time_header] = pd.to_datetime(df[time_header], format='%Y-%m-%d %H:%M:%S') \
                          + dt.timedelta(hours=hours, minutes=minutes)
    df[time_header] = df[time_header].dt.strftime(time_format_str)
    # Change time in filename
    old_filepath = filepath
    if change_fn is True:
        new_start_time = (pd.to_datetime('_'.join([os.path.split(filepath)[-1].split('_')[-3],
                                                   os.path.split(filepath)[-1].split('_')[-2]]),
                                         format='%Y%m%d_%H%M%S%f') +
                          dt.timedelta(hours=hours)).strftime('%Y%m%d_%H%M%S')+'00'
        path_list = [os.path.split(filepath)[0], '_'.join(['_'.join(os.path.split(filepath)[-1].split('_')[:-3]),
                                                           new_start_time, os.path.split(filepath)[-1].split('_')[-1]])]
        filepath = os.path.join(*path_list)
    # Save new file
    if header >= 1:
        df.to_csv(path_or_buf=filepath, index=False)
        with open(filepath, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.writelines(meta_data)
            f.write(content)
    else:
        df.to_csv(path_or_buf=filepath, index=False, header=False)
    return old_filepath, filepath


def infer_fc_log_type(fn):
    """
    Infers a flight controller log type from it's filename

    :param fn: Filename
    :type fn: str

    :raise ValueError: If the log extension is not .log or .json

    :return: File extension
    :rtype: str
    """
    _, ext = os.path.splitext(fn)
    if (ext != '.log') and (ext != '.json'):
        raise ValueError('%s is invalid fc log extension' % ext)
    else:
        return ext
