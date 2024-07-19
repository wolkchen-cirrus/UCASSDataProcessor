"""
This is the main user mechanism for interfacing with raw data in the program.
Data headers are described by the "data_flags" config setting, an example of
which is in Scripts/ShareInfo/ImportStructExample.json. Valid data flags for
import are set in the "valid_flags" config setting. Script will process files
between "dt[0]" and "dt[1]", or a single datetime if only dt is specified. dt
must be in format "YYYY-mm-dd HH:MM:SS" or, if two datetimes are specified,
"YYYY-mm-dd HH:MM:SS,YYYY-mm-dd HH:MM:SS".
"""

from oproc.ArchiveHandler import Utilities as utils
from oproc.ArchiveHandler import ImportLib as im
from oproc.ArchiveHandler.RawDataObjects.ImportObject import ImportObject
from oproc.ArchiveHandler.RawDataObjects.RawFile import RawFile
from oproc.ArchiveHandler.HDF5DataObjects.H5dd import H5dd
from oproc.ArchiveHandler.HDF5DataObjects.CampaignFile import CampaignFile
from oproc.ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict
from oproc import newprint

from argparse import ArgumentParser
import pandas as pd
import numpy as np
import datetime as dt
import inspect


print('####################################################')
print('######## Welcome to the generic data import ########')
print('####################################################')
print('')

# Redefining print function with timestamp
print = newprint()

# Parsing args
parser = ArgumentParser(description=__doc__)
parser.add_argument("-f", "--hdf5-filename", default=None,
                    help="hdf5 file to add to, will create new if needed")
parser.add_argument("dt", metavar="DATE",
                    help="Start and end date")
args = parser.parse_args()

if __name__ == "__main__":

    # Get datetime from input strings
    print(f'Parsed datetime: {args.dt}')
    if len(args.dt.split(',')) == 1:
        dts = pd.to_datetime(args.dt, format='ISO8601')
    elif len(args.dt.split(',')) == 2:
        dts = (pd.to_datetime(args.dt.split(',')[0],
                              format='ISO8601'),
               pd.to_datetime(args.dt.split(',')[1],
                              format='ISO8601'))
    else:
        raise ValueError('Invalid dt input')
    print(f'Processing datetime(s): {dts}')

    # Get iss from config
    iss = im.get_iss()
    # Infer types from import struct spec
    types = im.types_from_iss(iss)
    # Get frame of matching files to process
    fdf = utils.get_files(dts, types)

    # Loop through files using index
    h5_data = H5dd(None)
    for dt in fdf.index:

        # Reformat iss with fn keys and get object
        iss_o = im.get_iss_obj(iss, fdf, dt)

        print(f'Reading data from files {list(iss_o.dflags.keys())}')
        with RawFile(iss_o) as rf:
            data = rf.read()

        # Get instrument metadata from the metadata path specified in the conf
        md_obj = {}
        lfn = list(fdf.loc[[dt]].values.flatten())
        for cfn in lfn:
            if isinstance(cfn, str):
                pass
            elif np.isnan(cfn):
                continue
            else:
                cfn = str(cfn)
            sn = im.get_instrument_sn(cfn)
            if sn:
                md_obj = md_obj | im.read_instrument_data(sn)
                im.read_instrument_data(sn)
            else:
                continue

        # Next, assign the column data to the importer object. This is for
        # validation and quality assurance.
        d = list(data.values())[0]
        for dx in list(data.values())[1:]:
            d = d + dx
        d.date_time = dt
        i_obj = ImportObject(d.__get__())

        # Format into HDF5 dict for processing
        md = MatrixDict(i_obj.__dict__() | md_obj, unit_spec="default")
        md.date_time = dt
        h5_data = h5_data + H5dd(md)

    if args.hdf5_filename is None:
        print("No HDF5 file specified, quitting")
        raise SystemExit(0)

    print("Creating directory structure")
    utils.make_dir_structure()
    print("Writing data to file")
    h5fn = args.hdf5_filename
    with CampaignFile(h5fn, mode='a') as h5cf:
        try:
            h5cf.write(h5_data)
        except FileExistsError:
            print("Cannot overwrite group")
            raise SystemExit(1)

    #breakpoint()
    raise SystemExit(0)
