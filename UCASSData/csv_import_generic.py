"""
This is the main user mechanism for interfacing with raw data in the program.
Data headers are described by the "data_flags" config setting, an example of
which is in Scripts/ShareInfo/ImportStructExample.json. Valid data flags for
import are set in the "valid_flags" config setting. Script will process files
between "dt[0]" and "dt[1]", or a single datetime if only dt is specified. dt
must be in format "YYYY-mm-dd HH:MM:SS" or, if two datetimes are specified,
"YYYY-mm-dd HH:MM:SS,YYYY-mm-dd HH:MM:SS".
"""

from UCASSData.ArchiveHandler import Utilities as utils
from UCASSData.ArchiveHandler import ImportLib as im
from UCASSData.ArchiveHandler.RawDataObjects.ImportObject import ImportObject
from UCASSData.ArchiveHandler.RawDataObjects.RawFile import RawFile
from UCASSData.ArchiveHandler.HDF5DataObjects.H5dd import H5dd
from UCASSData.ArchiveHandler.HDF5DataObjects.CampaignFile import CampaignFile
from UCASSData.ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict

from argparse import ArgumentParser
import pandas as pd
from datetime import datetime

print('############################################')
print('#####Welcome to the generic data import#####')
print('############################################')
print('')


# Parsing args
parser = ArgumentParser(description=__doc__)
parser.add_argument("-f", "--hdf5-filename", default=None,
                    help="hdf5 file to add to, will create new if needed")
parser.add_argument("dt", metavar="DATE",
                    help="Start and end date")
args = parser.parse_args()

# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({datetime.now()})', *args, **kwargs)


print = timestamped_print

if __name__ == "__main__":

    # Get datetime from input strings
    print(f'Parsed datetime: {args.dt}')
    if len(args.dt.split(',')) == 1:
        dts = pd.to_datetime(args.dt, format='%Y-%m-%d %H:%M:%S')
    elif len(args.dt.split(',')) == 2:
        dts = (pd.to_datetime(args.dt.split(',')[0],
                              format='%Y-%m-%d %H:%M:%S'),
               pd.to_datetime(args.dt.split(',')[1],
                              format='%Y-%m-%d %H:%M:%S'))
    else:
        raise ValueError('Invalid dt input')

    # Get iss from config
    iss = im.get_iss()
    # Infer types from import struct spec
    types = im.types_from_iss(iss)
    # Get frame of matching files to process
    fdf = utils.get_files(dts, types)

    # Loop through files using index
    h5_data = H5dd(None, None)
    for dt in fdf.index:

        # Reformat iss with fn keys and get object
        iss_o = im.get_iss_obj(iss, fdf, dt)

        print(f'Reading data from files {list(iss_o.dflags.keys())}')
        with RawFile(iss_o) as rf:
            data = rf.read()

        # Get metadata from the raw files
        md_obj = im.metadata_from_rawfile_read(data, dt, description="test",
                                               sample_area=5e-7)

        # Next, assign the column data to the importer object. This is for
        # validation and quality assurance.
        d = list(data.values())[0]
        for dx in list(data.values())[1:]:
            d += dx
        d.date_time = dt
        i_obj = ImportObject(d.__get__())

        # Format into HDF5 dict for processing
        md = MatrixDict(i_obj.__dict__() | {"bbs": md_obj.__dict__()["bbs"]},
                        unit_spec="default")
        md.date_time = dt
        h5_data = h5_data + H5dd(md, md_obj)

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

    breakpoint()
    raise SystemExit(0)
