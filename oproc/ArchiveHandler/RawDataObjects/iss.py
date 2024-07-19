from ... import ConfigHandler as ch
from ... import newprint
import re


# Redefining print function with timestamp
print = newprint()


class iss(object):
    def __init__(self, data_flags: dict):
        self.__dflags = None
        self.dflags = data_flags
        self.uspec = self.__make_unit_spec(data_flags)

    @staticmethod
    def __finditem(obj, key):
        fields = []

        def __find_recursive(obj, key):
            if key in obj:
                fields.append(obj[key])
            for k, v in obj.items():
                if isinstance(v, dict):
                    item = __find_recursive(v, key)
                    if item is not None:
                        return item

        __find_recursive(obj, key)
        return fields

    def __make_unit_spec(self, data_flags: dict) -> dict:
        fields = self.__finditem(data_flags, 'data')
        fields = [s[i] for s in fields for i in s]
        c = [tuple(i) for s in fields for i in s if isinstance(i, list)]
        r = [(j, k[1]) for j, k in [i for s in [list(x.items()) for x in fields
                                                if isinstance(x, dict)]
                                    for i in s if isinstance(i[1], list)]]
        return dict(c + r)

    def __check_valid_cols(self, iss: dict, dkey: str = 'cols'):
        fields = self.__finditem(iss, dkey)
        fields = [i for s in fields for i in s if not isinstance(s, str)]
        vf = [x['name'] for x in ch.getval('valid_flags')]
        for flag in fields:
            if isinstance(flag, list):
                flag = flag[0]
            try:
                tag_suffix = ch.getval("tag_suffix")
                flag = flag.replace(re.search(r'(?=\d)\w+', flag).group(),\
                                    tag_suffix)
            except AttributeError:
                pass
            if flag not in vf:
                ch.getconf('valid_flags')
                if flag == '':
                    print('Skipping column')
                else:
                    raise ReferenceError('Data flag \'%s\' is not valid' % flag)
        print("All flags valid")

    @property
    def dflags(self) -> dict:
        return self.__dflags

    @dflags.setter
    def dflags(self, val: dict):
        self.__check_valid_cols(val)
        self.__dflags = val
