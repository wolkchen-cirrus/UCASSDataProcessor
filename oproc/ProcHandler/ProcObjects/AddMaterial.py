from .__Proc import Proc
from ... import newprint
import pandas as pd
import numpy as np
import os
from ...ArchiveHandler.RawDataObjects.iss import iss as imspec
from ...ArchiveHandler.RawDataObjects.RawFile import RawFile
from ...ArchiveHandler import ImportLib as im
from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ...ArchiveHandler.GenericDataObjects.MatrixColumn\
        import MatrixColumn as mc
from ... import ConfigHandler as ch


# Redefining print function with timestamp
print = newprint()


def get_ops_material_data():
    try:
        material = os.environ["WORKING_MATERIAL"]
        instrument = os.environ["WORKING_INSTRUMENT"]
        iss_path = os.environ["MATERIAL_ISS"]
    except KeyError:
        raise RuntimeError("env vars WORKING_MATERIAL, MATERIAL_ISS and\
                           WORKING_INSTRUMENT are not set, set these in\
                           main.py")

    mat_path = os.path.join(ch.getval('base_data_path'), 'Raw',
                            ch.getval('ops_material_folder'))
    mat_file = [x for x in os.listdir(mat_path) if material in x]
    mat_file = [x for x in mat_file if instrument in x]
    if not mat_file:
        raise FileNotFoundError
    elif len(mat_file) != 1:
        mat_file = [x for x in mat_file if "glmt" in x]
        if len(mat_file) != 1:
            raise LookupError("Multiple material files found")
        else:
            mat_file = mat_file[0]
    else:
        mat_file = mat_file[0]

    issd = im.get_iss_json(iss_path, obj=False)
    if len(issd) != 1:
        raise ValueError("iss must point to one file only")
    issd[mat_file] = issd.pop(list(issd.keys())[0])
    isso = imspec(issd)

    with RawFile(isso, nc=True) as rf:
        data = rf.read()

    if len(data) != 1:
        raise ValueError("iss must point to one file only")

    data = data[list(data.keys())[0]]
    for k, v in data.items():
        var = v
        if isinstance(v, mc):
            var = var.__get__()
        try:
            var = np.asarray(var).ravel()
        except AttributeError:
            pass
        data[k] = var
    return data


class AddMaterial(Proc):

    def setup(self):
        self.ivars = []
        self.ovars = ['mat_scs', 'mat_rad', 'density']
        self.unit_spec = {'mat_scs': 'm**2', 'mat_rad': 'um',\
                          'density':'kg m**-3'}

    def proc(self):
        matdat = get_ops_material_data()
        self.do = matdat
        return self.do

    def __repr__(self):
        return "AddMaterial"
