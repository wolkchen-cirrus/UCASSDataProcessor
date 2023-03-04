__author__ = "Jessica Girdwood"
__doc__ = """
This python module is responsible for handling the JSON config file, including
its initial creation.
"""

import json
import os.path
from pydoc import locate
from warnings import warn


config_fn = 'UCASSConfig.json'
config_path = os.path.join(os.path.split(os.path.abspath(__file__))[0], config_fn)


class _ConfVal:
    def __init__(self, cd):
        self.name = None
        self.val = None
        self.dtype = None
        self.unit = None
        self.desc = None
        [setattr(self, k, v) for k, v in cd.items() if k in self.__dict__]

    def check(self):
        if not isinstance(self.val, locate(self.dtype)):
            raise TypeError
        for k, v in self.__dict__.items():
            if v is None:
                raise AttributeError('%s is not set' % k)


def add_config(cd):
    while True:
        cc = _ConfVal(cd)
        cc.check()
        fcd = _read_conf()
        try:
            getconf(cc.name)
            raise FileExistsError("Config with name %s already exists" %
                                  cc.name)
        except AttributeError:
            pass

        try:
            fcd.append(cc.__dict__)
        except AttributeError:
            _blank_json()
            continue
        _write_over_json(fcd)
        break


def change_config_val(name, val):
    fcd = _read_conf()
    cc = _ConfVal(_getitem(name, fcd)[-1])
    cc.val = val
    cc.check()
    del_config(name)
    add_config(cc.__dict__)


def change_config(cd):
    cc = _ConfVal(cd)
    cc.check()
    del_config(cc.name)
    add_config(cc.__dict__)


def getval(name):
    fcd = _read_conf()
    cc = _ConfVal(_getitem(name, fcd)[-1])
    cc.check()
    return cc.val


def getconf(name):
    fcd = _read_conf()
    cc = _ConfVal(_getitem(name, fcd)[-1])
    cc.check()
    print(json.dumps(cc.__dict__, indent=4, separators=(',', ': ')))
    return cc.__dict__


def del_config(name):
    fcd = _read_conf()
    del fcd[_getitem(name, fcd)[0]]
    _write_over_json(fcd)


def _write_over_json(cd):
    with open(config_path, 'w') as cfp:
        try:
            json.dump(cd, cfp, indent=4, separators=(',', ': '))
        except TypeError:
            cd['dtype'] = cd['dtype'].__name__


def _read_conf():
    with open(config_path, 'r') as cfp:
        while True:
            try:
                fcd = json.load(cfp)
            except json.JSONDecodeError:
                _blank_json()
                continue
            break
    return fcd


def _getitem(name, fcd):
    item_list = [(i, x) for i, x in enumerate(fcd) if name == x['name']]
    if len(item_list) != 1:
        if len(item_list) > 1:
            raise AttributeError('More than one setting of name %s exists' % name)
        else:
            raise AttributeError('Name %s does not exist' % name)
    else:
        return item_list[0]


def _blank_json():
    warn('Creating blank JSON')
    with open(config_path, 'w') as cfp:
        cfp.write('[]')
