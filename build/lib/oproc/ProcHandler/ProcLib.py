"""
Library to store functions for processing data.
"""
import os
from .. import ConfigHandler as ch
from .. import newprint
import pandas as pd
import numpy as np
from .ProcObjects import DomainArray as da


print = newprint()


def get_material_data(material: str):

    mat_path = os.path.join(ch.getval('ucass_calibration_path'),
                            'MaterialData', 'StandardFormat')
    mat_file = [x for x in os.listdir(mat_path) if f'_{material}_' in x]
    if not mat_file:
        raise FileNotFoundError
    elif len(mat_file) != 1:
        mat_file = [x for x in mat_file if "_glmt" in x]
        if len(mat_file) != 1:
            raise LookupError("Multiple material files found")
        else:
            mat_file = mat_file[0]

    data = pd.read_csv(mat_file, header=1, names=["1", "2", "sca", "radii"],
                       usecols=[2, 3])
    data = data.to_dict(orient='list')

    return da("sca", np.matrix(data["sca"]).T, "radii",
              np.matrix(data["radii"]).T)
