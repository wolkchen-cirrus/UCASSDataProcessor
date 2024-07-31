from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ...ArchiveHandler.GenericDataObjects.MatrixColumn import MatrixColumn
from ...ArchiveHandler import ImportLib as im
from .. import ProcLib as pl
from ... import newprint

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
        self.__ivars = None
        self.__ovars = None

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

    def get_timevars(self):
        dd = self.di.__get__()
        return {'Time': dd['Time'], 'date_time': dd['date_time']}

    def get_ivars(self, dimless=False):
        """gets all the values for the ivars from md or kwargs"""
        md_dict = self.di.__get__()
        var_dict = {}
        for k in self.ivars:
            try:
                var = md_dict[k]
            except KeyError:
                var = self.args[k]
            if isinstance(var, MatrixColumn):
                var = var.__get__()
                if not isinstance(var, np.matrix):
                    raise TypeError("should be a matrix in here, somesthing is\
                                    wrong...")
            if dimless == True:
                try:
                    var = np.asarray(var).ravel()
                except AttributeError:
                    pass
            var_dict[k] = var
        return var_dict

    def __var_check(self):
        """Public self check to be called by procduring setup"""
        if (not self.ivars) or (not self.ovars) or (not self.unit_spec):
            raise ValueError("input and output tags, or unit spec,\
                             not specified")
        self.__var_search(self.ovars, self.args, self.di.__get__(),\
                          inverse=True)
        self.__var_search(self.ivars, self.args, self.di.__get__())

    @staticmethod
    def __var_search(var_list, arg_dict, arg_dict2, inverse=False):
        for var in var_list:
            if inverse:
                pl.not_require_vars(var, arg_dict)
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
            dd = val.__get__()
            output = self.di + val
        elif isinstance(val, dict):
            dd = val
            output = self.di.add_nc(val, self.unit_spec)
        else:
            raise TypeError
        pl.require_vars(self.ovars, dd)
        self.__do = output

    @property
    def ivars(self):
        return self.__ivars

    @ivars.setter
    def ivars(self, val):
        if not isinstance(val, list):
            raise TypeError
        else:
            self.__ivars = val

    @property
    def ovars(self):
        return self.__ovars

    @ovars.setter
    def ovars(self, val):
        if not isinstance(val, list):
            raise TypeError
        else:
            self.__ovars = val







