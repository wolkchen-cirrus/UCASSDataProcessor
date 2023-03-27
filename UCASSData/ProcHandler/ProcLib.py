"""
Library to store functions for processing data.
"""
import os
from .. import ConfigHandler as ch
from .ProcObjects import DomainArray as da


def get_material_data(material: str):

    mat_path = ch.getval('ucass_material_path')
    mat_file = None
    for fn in os.listdir(mat_path):
        if f'_{material}_' in fn:
            mat_file = fn
            break
    if not mat_file:
        raise FileNotFoundError
    mat_data = {}
    mat_data["sca"]
    return
