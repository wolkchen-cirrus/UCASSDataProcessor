from ...ArchiveHandler.GenericDataObjects.MatrixDict import MatrixDict as md
from ...ArchiveHandler.GenericDataObjects.MatrixColumn import MatrixColumn
from ... import newprint
from ... import ConfigHandler as ch
from ...ProcHandler import ProcLib as pl
from .PlotSpec import PlotSpec as ps
from ... import ureg

from typing import final
from matplotlib import pyplot as plt
import numpy as np
import ast
import inspect
import textwrap
import os
import pint


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
        self.__fig = None
        self.__ax = None

        # Valid args here are "mask:[list of masks as strings]"
        self.args = kwargs
        if not isinstance(plot_spec, ps):
            raise TypeError
        else:
            self.__plot_spec = plot_spec
        self.di = di
        self.plot_spec = plot_spec

        self.__var_check()
        self.__run_subclass_method("init_fig")
        self.__run_subclass_method("init_plot")

    def init_plot(self):
        mask = []
        try:
            mask = self.args['mask']
        except KeyError:
            pass
        data = self.get_ivars()
        pss = self.plot_spec.pss
        for r in range(self.shape[0]):
            for c in range(self.shape[1]):
                pss_i = pss[r, c]
                labels = pss_i.data
                n_lines = pss_i.n_lines
                n_dims = pss_i.n_dims
                for l in range(n_lines):
                    data_i = [data[x] for x in labels[l, :].tolist()]
                    if mask:
                        for m in mask:
                            data_i[0] = self.apply_plot_mask(m, data_i[0])
                            data_i[1] = self.apply_plot_mask(m, data_i[1])
                    else:
                        pass
                    if n_dims == 2:
                        self.__ax[r, c].plot(data_i[0], data_i[1])
                    else:
                        raise NotImplementedError("Not currently implemented \
                                                  for dimensions other than 2")

                    self.__ax[r,c].set_xlabel(self.get_disp_name(labels[l,0]))
                    self.__ax[r,c].set_ylabel(self.get_disp_name(labels[l,1]))

        self.__fig.suptitle(self.__repr__())

    def init_fig(self):
        if self.__fig:
            print("figure object exists")
            return
        else:
            fig, ax = plt.subplots(self.shape[0], self.shape[1],\
                                   layout='constrained')
            self.__fig = fig
            try:
                self.__ax = np.matrix(ax)
            except ValueError:
                self.__ax = np.matrix([ax])

    def apply_plot_mask(self, mask: np.matrix | str, data: np.matrix) \
            -> np.matrix:

        if isinstance(mask, str):
            try:
                mask = self.di.__get__()[mask].__get__()
            except KeyError:
                raise ValueError(f'mask {mask} not in matrix dict')
        if mask.shape != data.shape:
            raise ValueError("shapes don't match buddy")
        else:
            arr = np.multiply(mask, data).astype('float')
            arr[arr == 0] = np.nan
            return arr

    @staticmethod
    def get_disp_name(tag_name: str) -> str:
        tags = ch.getval('valid_flags')
        try:
            d_name = [x['disp'] for x in tags if x['name']==tag_name][0]
        except KeyError:
            d_name = [x['name'] for x in tags if x['name']==tag_name][0]
        try:
            unit = [x['unit'] for x in tags if x['name']==tag_name][0]
        except KeyError:
            unit = ''
        try:
            unit = ' (' + format(ureg[unit].u, '~P') + ')'
        except pint.errors.UndefinedUnitError:
            unit = ''
        return d_name + unit

    def __run_subclass_method(self, method_name: str, missing_error=False):
        __exists = False
        for cls in reversed(self.__class__.mro()):
            if hasattr(cls, method_name):
                method = getattr(cls, method_name)
                __exists = True
        if not __exists:
            if missing_error == True:
                raise AttributeError("method not implemented in subclass")
            elif hasattr(cls, method_name):
                method = getattr(self, method_name)
            else:
                raise AttributeError("method not implemented in class")

        method(self)

    def get_input_unit(self, var):
        return self.di.unit_spec[var]

    def get_ivars(self, dimless=False):
        """gets all the values for the ivars from md"""
        md_dict = self.di.__get__()
        tag_suffix = ch.getval("tag_suffix")
        var_dict = {}
        for kvar in self.plot_spec.ivars:
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
                        raise TypeError("should be a matrix in here, something is\
                                        wrong...")
                if dimless == True:
                    try:
                        var = np.asarray(var).ravel()
                    except AttributeError:
                        pass
                var_dict[k] = var
        return var_dict

    def __var_check(self):
        """Public self check to be called by plot during setup"""
        #if not self.unit_spec:
        #    raise ValueError("input and output tags, or unit spec,\
        #                     not specified")
        self.__var_search(self.plot_spec.ivars, self.di.__get__())

    @staticmethod
    def __var_search(var_list, arg_dict, inverse=False):
        for var in var_list:
            if inverse:
                pl.not_require_vars(var, arg_dict)
            else:
                pl.require_vars(var, arg_dict)

    #@final
    #def __self_check(self):
    #    """ensures valid subclass"""
    #    codeblock = textwrap.dedent(inspect.getsource(self.init_plot))
    #    if any(isinstance(node, ast.Return) for node in \
    #           ast.walk(ast.parse(codeblock))) is False:
    #        raise AttributeError("return not implemented in plot")

    def __len__(self):
        return self.__nplots

    def __repr__(self):
        return self.di.date_time

    @staticmethod
    def __valist(test: list, tp: str) -> bool:
        return all(isinstance(sub, locate(tp)) for sub in test)

    @property
    def args(self):
        return self.__args

    @args.setter
    def args(self, val):
        if isinstance(val, dict):
            self.__args = val
        else:
            raise TypeError

    @property
    def di(self):
        return self.__di

    @di.setter
    def di(self, val):
        if not isinstance(val, md):
            raise TypeError
        self.__di = val

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

    @property
    def plot_spec(self):
        return self.__plot_spec

    @plot_spec.setter
    def plot_spec(self, val):
        if isinstance(val, ps):
            self.__plot_spec = val
        else:
            raise TypeError

    @property
    def fig(self):
        return self.__fig

    @fig.setter
    def fig(self, val):
        self.__fig = val

    @property
    def ax(self):
        return self.__ax

    @ax.setter
    def ax(self, val):
        self.__ax = val





