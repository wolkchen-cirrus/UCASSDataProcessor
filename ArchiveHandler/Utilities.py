"""
Contains functions for data archive maintenance.
"""


import pandas as pd
import datetime as dt
import os


def csv_log_timedelta(filepath, hours, time_format_str, time_header, names=None,
                      change_fn=False, header=0, minutes=0):
    """
    a function to add or subtract a number of hours from a log file

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