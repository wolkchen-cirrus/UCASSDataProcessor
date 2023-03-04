__author__ = "Jessica Girdwood"
__copyright__ = "Copyright 2022, University of Hertfordshire and Subsidiary\
Companies"
__credits__ = ["J. M. Girdwood", "M. Y. Ruse"]
__license__ = "MIT"
__version__ = "0.0.1"
__maintainer__ = "J. Girdwood"
__email__ = "JessGirdwood@protonmail.com"
__status__ = "Development"
__doc__ = """
Processes data from the UCASS instrument, and manages the data repos.
"""

from UCASSData import *
from pint import UnitRegistry
import os.path


unit_file = os.path.join(os.path.split(os.path.abspath(__file__))[0],
                         'units.txt')
ureg = UnitRegistry()
ureg.load_definitions(unit_file)
