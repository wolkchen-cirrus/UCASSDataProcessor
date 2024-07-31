from .__Proc import Proc
from ... import newprint
import pandas as pd
import numpy as np
from ...ArchiveHandler.RawDataObjects.iss import iss as imspec
from ...ArchiveHandler.RawDataObjects.RawFile import RawFile
from ...ArchiveHandler import ImportLib as im
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ... import ConfigHandler as ch


# Redefining print function with timestamp
print = newprint()


def get_ops_material_data(material: str, instrument: str, iss_path: str):
    mat_path = ch.getval('base_data_path') + ch.getval('ops_material_folder')
    mat_file = [x for x in os.listdir(mat_path) if f'_{material}_' in x]
    mat_file = [x for x in mat_file if f'_{instrument}_' in x]
    if not mat_file:
        raise FileNotFoundError
    elif len(mat_file) != 1:
        mat_file = [x for x in mat_file if "glmt" in x]
        if len(mat_file) != 1:
            raise LookupError("Multiple material files found")
        else:
            mat_file = mat_file[0]

    issd = im.get_iss_json(iss_path, obj=False)
    if len(issd) != 1:
        raise ValueError("iss must point to one file only")
    issd[mat_file] = issd.pop(list(issd.keys())[0])
    isso = imspec(issd)

    with RawFile(isso, nc=True) as rf:
        data = rf.read()

    return pd.DataFrame.from_dict(data, orient='columns')


class AddMaterial(Proc):

    def setup(self):
        self.ivars = []
        self.ovars = ['mat_scs', 'mat_rad']
        self.unit_spec = {'mat_scs': 'um**-2', 'mat_rad': 'um'}

    def proc(self):
        mat_name = ch.getval('material')
        return self.do

    def __repr__(self):
        return "AddMaterial"
