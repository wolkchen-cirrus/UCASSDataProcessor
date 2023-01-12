__author__ = "Jessica Girdwood"
__copyright__ = "Copyright 2022, University of Hertfordshire and Subsidiary Companies"
__credits__ = ["J. Girdwood"]
__license__ = "MIT"
__version__ = "0.0.1"
__maintainer__ = "J. Girdwood"
__email__ = "j.girdwood@herts.ac.uk"
__status__ = "Development"
__doc__ = """
==============
ArchiveHandler
==============
This python module is responsible for handling the data archive; primarily, this includes importing csv data from 
various log files, and combining each data instance into an HDF5 file.
"""


import ArchiveHandler.CreateHDF5
