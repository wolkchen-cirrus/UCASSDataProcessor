__author__ = "Jessica Girdwood"
__copyright__ = "Copyright 2022, University of Hertfordshire and Subsidiary Companies"
__credits__ = ["J. Girdwood"]
__license__ = "MIT"
__version__ = "0.0.1"
__maintainer__ = "J. Girdwood"
__email__ = "j.girdwood@herts.ac.uk"
__status__ = "Development"
__doc__ = """
This python module is responsible for handling the JSON config file, including its initial creation.
"""

import json
import os


config_fn = 'UCASSConfig.json'
config_path = os.path.join(os.path.split(os.path.abspath(__file__))[0], config_fn)
base_config_dict = {
        'base_data_path': None,
        'ucass_calibration_path': None
    }


def check_config_file():
    raise NotImplemented


def create_base_json(overwrite=False):
    if overwrite is True:
        _write_over_json(base_config_dict)
    elif not os.path.exists(config_path):
        _write_over_json(base_config_dict)
    else:
        raise FileExistsError('Config JSON already exists at %s' % config_path)


def read_config_key(key_name):
    config_dict = _read_json()
    return config_dict[key_name]


def change_config_val(key_name, key_val):
    config_dict = _read_json()
    if key_name in config_dict.keys():
        config_dict[key_name] = key_val
        _write_over_json(config_dict)
    else:
        raise KeyError('%s is not a valid config variable' % key_name)


def _read_json():
    with open(config_path, 'r') as config_file_pointer:
        config_dict = json.load(config_file_pointer)
        config_file_pointer.close()
    return config_dict


def _write_over_json(new_dict):
    config_json = json.dumps(new_dict)
    with open(config_path, 'w') as config_file_pointer:
        config_file_pointer.write(config_json)
        config_file_pointer.close()
