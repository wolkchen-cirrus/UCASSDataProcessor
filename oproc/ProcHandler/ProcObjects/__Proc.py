from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ...ArchiveHandler import ImportLib as im
from .. import ProcLib as pl
from ... import newprint
from .__UnitArray import UnitArray as ua

from typing import final
import pandas as pd
import numpy as np
import ast
import inspect
import textwrap


# Redefining print function with timestamp
print = newprint()


class Proc(object):
    """
    Base processor object. User-defined processes must be subclasses of this
    object.

    :param di: MatrixDict object and data input.
    """

    @final
    def __init__(self, di: md, **kwargs):

        self.__di = None
        self.__do = None
        self.unit_spec = None
        self.args = kwargs
        self.ivars = None
        self.ovars = None

        self.__self_check()
        self.di = di

        setup_exists = False
        for cls in reversed(self.__class__.mro()):
            if hasattr(cls, 'setup'):
                cls.setup(self)
                setup_exists = True
        if not setup_exists:
            raise AttributeError("setup not implemented in subclass")
        self.__var_check()

    def setup(self):
        print("Running setup")
        pass

    def proc(self):
        raise NotImplementedError("This object should be overwritten by\
                                  subclass")

#    def __getcols(self, tags: list | str) -> dict:
#        """Gets columns with specified tags, must be in col_dict"""
#        if isinstance(tags, str):
#            tags = [tags]
#        [im.check_flags(x) for x in tags]
#        df = self.di.df()
#        tags = [im.tag_generic_to_numeric(x, df.columns) for x in tags]
#        df = df[tags]
#        return self.__df_to_ual(df)
#
#    def __df_to_uad(self, df: pd.DataFrame) -> dict:
#        dfd = df.to_dict(orient='list')
#        return {k: ua(k, np.matrix(v).T, len(v)) for k, v in dfd.items()}

    def __var_check(self):
        """Public self check to be called by procduring setup"""
        if (not self.ivars) or (not self.ovars) or (not self.unit_spec):
            raise ValueError("input and output tags, or unit spec,\
                             not specified")
        self.__var_search(self.ovars, self.args, self.di.__get__(),\
                          inverse=True)
        self.__var_search(self.ivars, self.args, self.di.__get__())

    @staticmethod
    def __var_search(var, arg_dict, arg_dict2, inverse=False):
        if inverse:
            try:
                pl.not_require_vars(var, arg_dict)
            except ValueError:
                pl.not_require_vars(var, arg_dict2)
        else:
            try:
                pl.require_vars(var, arg_dict)
            except ValueError:
                pl.require_vars(var, arg_dict2)


    @final
    def __self_check(self):
        """ensures valid subclass"""
        codeblock = textwrap.dedent(inspect.getsource(self.proc))
        if any(isinstance(node, ast.Return) for node in \
               ast.walk(ast.parse(codeblock))) is False:
            raise AttributeError("return not implemented in __proc")

    def __len__(self):
        return len(self.__di)

    @property
    def di(self):
        """data input"""
        return self.__di

    @di.setter
    def di(self, val):
        if not isinstance(val, md):
            raise TypeError
        self.__di = val

    @property
    def do(self):
        """data output"""
        return self.__do

    @do.setter
    def do(self, val):
        if isinstance(val, md):
            dd = list(val.__get__().keys())
            output = self.di + val
        elif isinstance(val, dict):
            dd = val
            output = self.di.add_nc(val, self.unit_spec)
        else:
            raise TypeError
        for ovar in self.ovars:
            if ovar not in dd:
                raise ValueError(f'ovar {ovar} not in vars {val}')
        self.__do = output







