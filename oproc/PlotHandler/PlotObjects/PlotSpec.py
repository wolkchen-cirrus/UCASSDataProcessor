import pandas as pd
import numpy as np


class PlotSpec(object):
    def __init__(
                    self, nrows: int, ncols: int,\
                    key_list: np.ndarray | dict[list[int]] | None | int = None,
                    dim_list: np.ndarray | dict[list[int]] | None | int = None
                ):

        self.__shape = None
        self.__plot_spec = None
        self.__dim_list = None

        self.dim_list = dim_list
        self.plot_spec = key_list
        self.shape = (nrows, ncols)

    @staticmethod
    def __blank_df(nrows, ncols, val):
        return pd.DataFrame(np.ones([nrows, ncols], int) * val)

    @staticmethod
    def __valist(din: list, tp: str):
        return all(isinstance(sub, locate(tp)) for sub in din)

    @staticmethod
    def __set_spec(val):
        if isinstance(val, dict):
            tval = list(val.values())
            flat_tval = [x for xs in tval for x in xs]
            if len(set(map(len, val.values()))) != 1:
                raise ValueError
            elif not self.__valist(tval, "list"):
                raise ValueError
            elif not self.__valist(flat_tval, "int"):
                raise ValueError
            else:
                return pd.DataFrame(val)
        elif isinstance(val, int):
            return self.__blank_df(self.shape[0], self.shape[1], val)
        elif isinstance(val, np.ndarray):
            return pd.DataFrame(val)
        elif val is None:
            return self.__blank_df(self.shape[0], self.shape[1], 1)
        else:
            raise TypeError

    @property
    def plot_spec(self):
        return self.__plot_spec

    @plot_spec.setter
    def plot_spec(self, val):
        self.__plot_spec = self.__set_spec(val)

    @property
    def dim_list(self):
        return self.__dim_list

    @dim_list.setter
    def dim_list(self, val):
        self.__dim_list = self.__set_spec(val)

    @property
    def shape(self):
        return self.__shape

    @shape.setter
    def shape(self, val):
        if not isinstance(val, tuple):
            raise TypeError
        elif len(val) != 2:
            raise ValueError
        elif not self.__valist(val, 'int'):
            raise TypeError
        else:
            self.__shape = val





