"""
Contains functions for data archive maintenance and searching. Anything to do
with raw data maintenance goes here.
"""

import pandas as pd
import datetime as dt
import numpy as np
from .. import ConfigHandler as ch
from . import ImportLib as im
import os.path
from datetime import datetime


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({datetime.now()})', *args, **kwargs)


print = timestamped_print


def get_log_path(path: str | None, t: str) -> str:
    """
    Returns abs path from log filename and type

    :param path: file path, abs or rel
    :param t: data type (Met, FC, UCASS, &c.)

    :raise TypeError: If the calculated path does not exist

    :return: the abs path
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


def match_raw_files(match_types: list | str, files: list | str | None = None,
                    default_type: str = 'UCASS',
                    tol_min: int = 20) -> pd.DataFrame:
    """
    A function to find matching raw files by datetime, assuming some data for
    one data instance were in different files.

    :param files: list of files you want to match up
    :param match_types: folder name in the "Raw" directory
    :param tol_min: tolerance of matches in minutes
    :param default_type: if files is not specified, type used to get matches

    :return: dataframe of matches where the index is a datetime index.
    """
    if files:
        files = im.to_list(files)
    else:
        files = os.listdir(get_log_path(None, default_type))
    match_types = im.to_list(match_types)
    # Get datetime from files
    dt0s = im.fn_datetime(files)
    # Make df with dt index for storage (match frame)
    mf = pd.DataFrame(index=pd.DatetimeIndex(dt0s))
    # mt can be 'Met', 'UCASS', &c
    for mt in match_types:
        # mfn is a list of the matched file names
        mfn = []
        # dt0 is the datetime; in_file is the input file
        for dt0 in dt0s:
            # List of potential matches
            tm = os.listdir(get_log_path(None, mt))
            # Get match dts
            fdt = im.fn_datetime(tm)
            # Get deltas
            delta_dt = [abs((x - dt0).total_seconds() / 60.0) for x in fdt]
            if min(delta_dt) > tol_min:
                # Fill with nans if tol is breached
                mfn.append(np.nan)
            else:
                # Append filename using index to match
                mfn.append(tm[list(delta_dt).index(min(delta_dt))])
        # Write col to dataframe
        mf[mt] = mfn
    # Return sorted dataframe
    return mf.sort_index()


def make_dir_structure():
    """
    A function to create the data storage structure required for the software.
    If directories already exist, the function skips them. The config json is
    used to find the base path and structure.
    """
    def make_dirs(fd: dict, root: str):
        """
        Recursive function to search for a directory structure and create
        missing directories.

        :param fd: directory dict
        :param root: base directory (abs)
        """
        for key, val in fd.items():
            path = os.path.join(root, key)
            try:
                os.mkdir(path)
            except FileExistsError:
                print('Directory \"%s\" already exists; skipping directory'
                      % path)
            if isinstance(val, dict):
                make_dirs(val, os.path.join(root, key))

    base = ch.getval('base_data_path')
    if not os.path.exists(base):
        raise FileNotFoundError('Base path \'%s\' in config does not exist'
                                % base)
    struct = ch.getval('dir_structure')
    make_dirs(struct, base)


def csv_log_timedelta(filepath: str, hours: int,
                      time_format_str: str, time_header: str,
                      names: list = None, change_fn: bool = False,
                      header: int = 0, minutes: int = 0) -> tuple:
    """
    A function to add or subtract a number of hours from a log file

    :param filepath: Absolute file path
    :param hours: Number of hours to add or subtract
    :param minutes: Number of hours to add or subtract
    :param time_format_str: Input datetime format string
    :param time_header: Which header specified in 'names' is the time column
    :param names: Column names for data frame
    :param change_fn: Change filename label?
    :param header: Which row are the headers on, 0 if no headers

    :returns: The old and new (if changed) filepaths as a tuple
    """
    # Change time in file column
    df = pd.read_csv(filepath, delimiter=',', header=header, names=names)\
        .dropna()
    if header >= 1:
        with open(filepath) as f:
            meta_data = f.readlines()[:header]
    try:
        df[time_header] = pd.to_datetime(df[time_header],
                                         format=time_format_str) \
            + dt.timedelta(hours=hours, minutes=minutes)
    except ValueError:
        df[time_header] = pd.to_datetime(df[time_header],
                                         format='%Y-%m-%d %H:%M:%S') \
            + dt.timedelta(hours=hours, minutes=minutes)
    df[time_header] = df[time_header].dt.strftime(time_format_str)
    # Change time in filename
    old_filepath = filepath
    if change_fn is True:
        new_start_time = (im.fn_datetime(filepath)
                          + dt.timedelta(hours=hours))\
            .strftime('%Y%m%d_%H%M%S')\
            + '00'
        path_list = [os.path.split(filepath)[0],
                     '_'.join(['_'.join(os.path.split(filepath)[-1]
                                        .split('_')[:-3]), new_start_time,
                               os.path.split(filepath)[-1].split('_')[-1]])]
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


def infer_log_type(fn: str) -> str:
    """
    Infers a log type from it's filename

    :param fn: Filename

    :raise ValueError: If the log extension is neither .log, .json, nor .csv

    :return: File extension
    """
    _, ext = os.path.splitext(fn)
    if (ext != '.log') and (ext != '.json') and (ext != '.csv'):
        raise ValueError('%s is invalid fc log extension' % ext)
    else:
        return ext


def get_files(dts: tuple, types: list[str]) -> pd.DataFrame:
    """
    gets files from datetime or between two datetime vars; primarily invokes
    match_raw_files

    :param dts: date times
    :param types: list of file types ('UCASS', 'Met', &c)

    :return: reduced match types array
    """
    types = im.to_list(types)
    df = match_raw_files(types)
    if isinstance(dts, tuple):
        return df.iloc[df.index.get_indexer([dts[0]], method='nearest')[0]:
                       df.index.get_indexer([dts[-1]], method='nearest')[0]]
    else:
        return df.iloc[df.index.get_indexer([dts], method='nearest')[0]:
                       df.index.get_indexer([dts], method='nearest')[0] + 1]
