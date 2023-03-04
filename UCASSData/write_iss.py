from UCASSData import ConfigHandler as ch

import json
from argparse import ArgumentParser


parser = ArgumentParser(description=__doc__)
parser.add_argument("ssp", default=None,
                    help="path to the import struct spec json")
args = parser.parse_args()

ssp = args.ssp
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
    ch.change_config_val("data_flags", iss)
