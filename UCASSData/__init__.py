__author__ = "Jessica Girdwood"
__credits__ = ["J. M. Girdwood", "M. Y. Ruse"]
__license__ = "MIT"
__version__ = "0.0.2"
__maintainer__ = "J. Girdwood"
__email__ = "JessGirdwood@protonmail.com"
__status__ = "Development"
__doc__ = """
Processes data from OPC instruments, and manages the data repos.
"""

from UCASSData import *
from pint import UnitRegistry
import os.path
from datetime import datetime


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({datetime.now()})', *args, **kwargs)


print = timestamped_print


unit_file = os.path.join(os.path.split(os.path.abspath(__file__))[0],
                         'units.txt')
ureg = UnitRegistry()
ureg.load_definitions(unit_file)
ureg.default_format = "~"
