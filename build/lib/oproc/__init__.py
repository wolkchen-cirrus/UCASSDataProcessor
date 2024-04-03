__author__ = "Jessica Girdwood"
__credits__ = ["J. M. Girdwood"]
__license__ = "MIT"
__version__ = "0.0.2"
__maintainer__ = "J. Girdwood"
__email__ = "JessGirdwood@protonmail.com"
__status__ = "Development"
__doc__ = """
Processes data from OPC instruments, and manages the data repos.
"""

from oproc import *
from pint import UnitRegistry
import os.path
import datetime as dt
import ConfigHandler as ch


def newprint():
    old_print = print

    def __round_seconds(obj: dt.datetime) -> dt.datetime:
        if obj.microsecond >= 500_000:
            obj += dt.timedelta(seconds=1)
        return dt.datetime.timestamp(obj.replace(microsecond=0))

    def timestamped_print(*args, **kwargs):
        old_print(f'[\x1b[32m{__round_seconds(dt.datetime.now())}\x1b[0m]',
                  *args, **kwargs)

    return timestamped_print


unit_file = os.path.join(os.path.split(os.path.abspath(__file__))[0],
                         'units.txt')
ureg = UnitRegistry()
ureg.load_definitions(unit_file)
ureg.default_format = "~"

tag_suffix = ch.getval("tag_suffix")
