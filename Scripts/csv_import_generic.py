"""
This is the main user mechanism for interfacing with raw data in the program.
Data headers are described by the "data_flags" config setting, an example of
which is in Scripts/ShareInfo/ImportStructExample.json. Valid data flags for
import are set in the "valid_flags" config setting. Script will process files
between "dt[0]" and "dt[1]", or a single datetime if only dt is specified. dt
must be in format "YYYY-mm-dd HH:MM:SS" or, if two datetimes are specified,
"YYYY-mm-dd HH:MM:SS,YYYY-mm-dd HH:MM:SS".
"""

from argparse import ArgumentParser
import UCASSData.ArchiveHandler.Utilities as utils
import UCASSData.ArchiveHandler.ImportLib as im
import UCASSData.ConfigHandler as ch
import json
import os
import pandas as pd


parser = ArgumentParser(description=__doc__)
parser.add_argument("-ssp", "--struct-spec-path", default=None,
                    help="path to the import struct spec json")
parser.add_argument("-a", "--append-file", default=None,
                    help="hdf5 file to add to, if None will create new")
parser.add_argument("dt", metavar="DATE",
                    help="Start and end date")
args = parser.parse_args()

if __name__ == "__main__":
    # Get datetime from input strings
    if len(args.dt.split(',')) == 1:
        dts = pd.to_datetime(args.dt, format='%Y-%m-%d %H:%M:%S')
    elif len(args.dt.split(',')) == 2:
        dts = (pd.to_datetime(args.dt.split(',')[0],
                              format='%Y-%m-%d %H:%M:%S'),
               pd.to_datetime(args.dt.split(',')[1],
                              format='%Y-%m-%d %H:%M:%S'))
    else:
        raise ValueError('Invalid dt input')
    # Get import struct spec
    if not args.struct_spec_path:
        ssp = os.path.join(os.getcwd(), 'ImportStructSpec.json')
    else:
        ssp = args.struct_spec_path
    with open(ssp, 'r') as ssp:
        iss = json.load(ssp)
    try:
        ch.add_config({"name": "data_flags",
                       "val": iss,
                       "dtype": "dict",
                       "unit": "n/a",
                       "desc": "flags for data headers, read "
                               "\"valid_flags\" config entry for details"
                       })
        print("Written iss to config:")
        ch.getconf("data_flags")
    except FileExistsError:
        pass
    # Sort iss for priority assignment
    k = list(iss.keys())
    k.sort(reverse=True)
    iss = {i: iss[i] for i in k}
    # Infer types from import struct spec
    types = [iss[x]['type'] for x in list(iss.keys())]
    # Get frame of matching files to process
    fdf = utils.get_files(dts, types)
    # Loop through files using index
    data = {}
    for dt in fdf.index:
        # Reformat iss with fn keys
        iss_n = {}
        for k in iss:
            # Get type from iss
            t = iss[k]['type']
            # Get filename from df
            fn = fdf[t][dt]
            # Rename the dict key
            iss_n[fn] = iss[k]
        # Save or add iss
        ch.change_config_val("data_flags", iss_n)
        # Get raw data from files
        data[dt] = im.proc_iss(iss_n)

    pass
