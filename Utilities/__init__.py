__author__ = "Jessica Girdwood"
__copyright__ = "Copyright 2022, University of Hertfordshire and Subsidiary Companies"
__credits__ = ["J. Girdwood"]
__license__ = "MIT"
__version__ = "0.0.1"
__maintainer__ = "J. Girdwood"
__email__ = "j.girdwood@herts.ac.uk"
__status__ = "Development"
__doc__ = """
This python module contains misc tools to be used by the UCASSDataProcessor master package.
"""


import pandas as pd
import datetime as dt
import os


def csv_log_timedelta(filepath, hours, time_format_str, out_time_format_str, time_header, names,
                      change_fn=False, header=0):
    """
    a function to add or subtract a number of hours from a log file

    :param filepath: Absolute file path
    :type filepath: str
    :param hours: Integer number of hours to add or subtract
    :type hours: int
    :param time_format_str: Input datetime format string
    :type time_format_str: str
    :param out_time_format_str: Output datetime format string
    :type out_time_format_str: str
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
    df[time_header] = pd.to_datetime(df[time_header], format=time_format_str) + dt.timedelta(hours=hours)
    df[time_header] = df[time_header].dt.strftime(out_time_format_str)
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
    if header == 0:
        df.to_csv(path_or_buf=filepath, index=False, header=False)
    else:
        df.to_csv(path_or_buf=filepath)
    return old_filepath, filepath
