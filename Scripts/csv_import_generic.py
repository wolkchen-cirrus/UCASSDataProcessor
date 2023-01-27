"""
This is the main user mechanism for interfacing with raw data in the program. Data headers are described by the
"data_flags" config setting, an example of which is in Scripts/ShareInfo/ImportStructExample.json. Valid data flags for
import are set in the "valid_flags" config setting. Script will process files between "--s-dt" and "dt", or a single
datetime if only dt is specified. dt must be in format "YYYY-mm-dd HH:MM:SS".
"""

from argparse import ArgumentParser
import UCASSData.ArchiveHandler.Utilities as utils
import json
import os
import pandas as pd


parser = ArgumentParser(description=__doc__)
parser.add_argument("-ssp", "--struct-spec-path", default=None, help="path to the import struct spec json")
parser.add_argument("-a", "--append-file", default=None, help="hdf5 file to add to, if None will create new")
parser.add_argument("dt", metavar="DATE", help="Start and end date: comma separated dt strings, or single dt string")
args = parser.parse_args()

if __name__ == "__main__":
    # Get datetime from input strings
    if len(args.dt.split(',')) == 1:
        dts = pd.to_datetime(args.dt, format='%Y-%m-%d %H:%M:%S')                       # Only datetime
    elif len(args.dt.split(',')) == 2:
        dts = (pd.to_datetime(args.dt.split(',')[0], format='%Y-%m-%d %H:%M:%S'),       # Start datetime
               pd.to_datetime(args.dt.split(',')[1], format='%Y-%m-%d %H:%M:%S'))       # End datetime
    else:
        raise ValueError('Invalid dt input, must be dt string or list thereof in format YYYY-mm-dd HH:MM:SS')
    # Get import struct spec
    if not args.struct_spec_path:
        ssp = os.path.join(os.getcwd(), 'ImportStructSpec.json')
    else:
        ssp = args.struct_spec_path
    with open(ssp, 'r') as ssp:
        iss = json.load(ssp)
    # Infer types from import struct spec
    types = [iss[x]['type'] for x in list(iss.keys())]
    # Get frame of matching files to process
    fdf = utils.get_files(dts, types)
    pass
