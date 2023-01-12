import pandas as pd
import datetime as dt
import os


def csv_log_timedelta(filepath, hours, time_format_str, out_time_format_str, time_header, names,
                      change_fn=False, header=0):
    """
    a function to add or subtract a number of hours from a log file
    ...

    Parameters:
    -----------
    :param filepath - absolute file path
    :param hours - integer number of hours to add or subtract
    :param time_format_str - input datetime format string
    :param out_time_format_str - output datetime format string
    :param time_header - string, which header specified in 'names' is the time column
    :param names - column names for data frame
    :param change_fn - change filename label? boolean true or false
    :param header - which row are the headers on, 0 if no headers
    ...

    Returns:
    --------
    :returns - the old and new (if changed) filepaths as a tuple
    """
    # Change time in file column
    df = pd.read_csv(filepath, delimiter=',', header=header, names=names).dropna()
    df[time_header] = pd.to_datetime(df[time_header], format=time_format_str) + dt.timedelta(hours=hours)
    df[time_header] = df[time_header].dt.strftime(out_time_format_str)
    # Change time in filename
    old_filepath = filepath
    if change_fn is True:
        new_start_time = (pd.to_datetime('_'.join([os.path.split(filepath)[-1].split('_')[-3],
                                                   os.path.split(filepath)[-1].split('_')[-2]]), format='%Y%m%d_%H%M%S%f') +
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
