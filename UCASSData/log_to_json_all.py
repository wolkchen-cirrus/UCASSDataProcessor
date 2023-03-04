"""
Converts all the flight logs to json databases for quick searching
"""

import os
from argparse import ArgumentParser
from . import ConfigHandler as ch
from .ArchiveHandler import Utilities as utils
from .ArchiveHandler import MavLib as MavLib


base = ch.getval('base_data_path')
parser = ArgumentParser(description=__doc__)
parser.add_argument("-d", "--in-directory", default='FC', metavar="LOG DIR")
parser.add_argument("-od", "--out-directory", default='FC Proc',
                    metavar="JSON DIR")
args = parser.parse_args()

in_dir = utils.get_log_path(None, args.in_directory)
out_dir = utils.get_log_path(None, args.out_directory)

if not os.path.exists(out_dir):
    print('Creating data directory structure at base path %s' % base)
    utils.make_dir_structure()

if __name__ == '__main__':
    log_files = os.listdir(in_dir)
    file_df = utils.match_raw_files([args.in_directory, args.out_directory],
                                    files=log_files, tol_min=1)
    proc_list = file_df.isna()[args.in_directory]
    [file_df.isna()[args.out_directory] is True].values.tolist()
    if not proc_list:
        raise FileNotFoundError('All .log files are processed')
    for f in proc_list:
        MavLib.log_to_json(f)
