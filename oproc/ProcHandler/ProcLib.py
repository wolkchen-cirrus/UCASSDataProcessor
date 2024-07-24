"""
Library to store functions for processing data.
"""
import os
import re
from .. import ConfigHandler as ch
from .. import newprint
import pandas as pd
import numpy as np
from .ProcObjects import DomainArray as da


print = newprint()


def get_all_suffix(var: str, din: dict) -> dict:
    tag_suffix = ch.getval("tag_suffix")
    if tag_suffix not in var:
        raise ValueError(f"tag suffix is not in var {var}")
    bval = var.replace(tag_suffix, '')
    i = 1
    dout = {}
    while True:
        test_str = bval + str(i)
        try:
            dout[test_str] = din[test_str]
        except KeyError:
            break
        i = i+1
    if not dout:
        raise ValueError(f"suffix value {var} not in data")
    else:
        return dout


def require_vars(var_list: list, kwargs: dict):
    for var in var_list:
        tests = list(kwargs.keys())
        tag_suffix = ch.getval("tag_suffix")
        if tag_suffix in var:
            to = []
            for test in tests:
                try:
                    to.append(test.replace(
                        re.search(r'(?=\d)\w+', test).group(), tag_suffix))
                except AttributeError:
                    to.append(test)
            tests = to
        present = False
        for k in tests:
            if k == var:
                present = True
        if present is False:
            raise ValueError(f'variable {var} not present in kwargs')
    print('Var check passed')
    return


def not_require_vars(var_list: list, kwargs: dict):
    try:
        require_vars(var_list, kwargs)
    except ValueError:
        print('Var check passed')
        return
    raise ValueError(f'Output variable already in struct')


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
