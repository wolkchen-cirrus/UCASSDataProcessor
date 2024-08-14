from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ...ArchiveHandler.GenericDataObjects.MatrixColumn import MatrixColumn
from ... import newprint
from ... import ConfigHandler as ch

from typing import final
from matplotlib import pyplot as plt
import numpy as np
import ast
import inspect
import textwrap


# Redefining print function with timestamp
print = newprint()


class Plot(object):
    """
    Base plotting object. User-defined processes must be subclasses of this
    object.

    :param di: MatrixDict object and data input.
    """

    @final
    def __init__(self, di: md, plot_spec: ps, **kwargs):

        try:
            style = os.environ['PLOT_STYLE']
        except KeyError:
            raise RuntimeError('env var PLOT_STYLE is not set properly')
        plt.style.use(f'oproc.PlotHandler.Styles.{style}')

        self.__di = None
        self.__ivars = None
        self.__plot_spec = None

        self.args = kwargs
        self.plot_spec = plot_spec
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
        #TODO: Run some kind of init figure method here

#    def setup(self):
#        print("Running setup")
#        pass

    def plot(self, ivars):
        raise NotImplementedError("This object should be overwritten by\
                                  subclass")

    def get_input_unit(self, var):
        return self.di.unit_spec[var]

    def get_ivars(self, dimless=False):
        """gets all the values for the ivars from md or kwargs"""
        md_dict = self.di.__get__()
        tag_suffix = ch.getval("tag_suffix")
        var_dict = {}
        for kvar in self.ivars:
            if tag_suffix in kvar:
                suffix_vars = pl.get_all_suffix(kvar, md_dict)
                klist = list(suffix_vars.keys())
            else:
                klist = [kvar]
            for k in klist:
                var = md_dict[k]
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
        if not self.unit_spec:
            raise ValueError("input and output tags, or unit spec,\
                             not specified")
        self.__var_search(self.ivars, self.di.__get__())

    @staticmethod
    def __var_search(var_list, arg_dict, inverse=False):
        for var in var_list:
            if inverse:
                pl.not_require_vars(var, arg_dict)
            else:
                pl.require_vars(var, arg_dict)

    @final
    def __self_check(self):
        """ensures valid subclass"""
        codeblock = textwrap.dedent(inspect.getsource(self.plot))
        if any(isinstance(node, ast.Return) for node in \
               ast.walk(ast.parse(codeblock))) is False:
            raise AttributeError("return not implemented in plot")

    def __len__(self):
        return self.__nplots

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
    def ivars(self):
        return self.__ivars

    @ivars.setter
    def ivars(self, val):
        if not isinstance(val, np.matrix):
            raise TypeError
        elif val.shape[0] != self.__nplots:
            raise ValueError
        else:
            self.__ivars = val

    @property
    def handle(self):
        return self.__handle

    @handle.setter
    def handle(self, val):
        if not isinstance(val, plt.Figure):
            raise TypeError
        else:
            self.__handle = val

    @property
    def shape(self):
        return self.plot_spec.shape











