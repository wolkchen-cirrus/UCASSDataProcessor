__author__ = "Jessica Girdwood"
__doc__ = """
This python module is responsible for handling the data archive; primarily, 
this includes importing csv data from various log files, and combining each 
data instance into an HDF5 file.
"""


from UCASSData.ArchiveHandler import *
from pint import UnitRegistry
import os.path


unit_file = os.path.join(os.path.split(os.path.abspath(__file__))[0],
                         'units.txt')
ureg = UnitRegistry()
ureg.load_definitions(unit_file)
