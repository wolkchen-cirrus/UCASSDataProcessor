import pandas as pd
import numpy as np


class __PSS(object):
    def __init__(self, pssd: dict):
        self.data = None
        self.n_lines = None
        self.n_data = None
        [setattr(self, k, v) for k, v in pssd.items() if k in self.__dict__]
        self.__check()

    def __check(self):
        if not isinstance(self.val, locate(self.dtype)):
            raise TypeError
        for k, v in self.__dict__.items():
            if v is None:
                raise AttributeError('%s is not set' % k)


class PlotSpec(object):
    def __init__(self, nrows: int, ncols: int, din: dict[list[str]] \
                plot_spec: np.ndarray | dict[list[int]] | None | int = None, \
                dim_list: np.ndarray | dict[list[int]] | None | int = None):

        self.__shape = None
        self.__plot_spec = None
        self.__dim_list = None
        self.__din = None
        self.__pss = None

        self.shape = (nrows, ncols)
        self.din = din
        self.dim_list = dim_list
        self.plot_spec = plot_spec
        self.pss = self.__make_pss()

        if (self.dim_list.shape != self.shape) or \
           (self.plot_spec != self.shape):
            raise ValueError

    def __get__(self):
        return self.pss

    def __make_pss(self) -> np.ndarray:

        def __make_pss_entry(din, dim, ndat) -> dict:
            return {"data": din, "n_dims": dim, "n_lines": ndat}

        pss = []
        for row in range(self.shape[0]):
            pssrw = []
            for col in range(self.shape[1]):
                pss_inst = __PSS(__make_pss_entry(self.din[row, col],\
                                              self.dim_list[row, col],\
                                              self.plot_spec[row, col]))
                pssrw.append(pss_inst)
            pss.append(pssrw)
        return np.array(pss)

    @staticmethod
    def __blank_df(nrows, ncols, val) -> pd.DataFrame:
        return pd.DataFrame(np.ones([nrows, ncols], int) * val)

    @staticmethod
    def __valist(test: list, tp: str) -> bool:
        return all(isinstance(sub, locate(tp)) for sub in test)

    def __check_din(self, val) -> bool:
        if len(val) != np.prod(self.shape):
            return False
        dims = pd.DataFrame(self.dim_list).to_dict(orient='tight')['data']
        dims = [x for xs in dims for x in xs]
        for v, d in zip(val, dims):
            if not isinstance(v, list):
                return False
            elif len(v) != d:
                return False
            else:
                continue
        return True

    @staticmethod
    def __set_spec(val, base_type="int"):
        if isinstance(val, dict):
            tval = list(val.values())
            flat_tval = [x for xs in tval for x in xs]
            if len(set(map(len, val.values()))) != 1:
                raise ValueError
            elif not self.__valist(tval, "list"):
                raise ValueError
            elif not self.__valist(flat_tval, base_type):
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
        ps = self.__set_spec(val)
        if ps.shape != self.shape:
            raise ValueError
        else:
            self.__plot_spec = ps.to_numpy()

    @property
    def dim_list(self):
        return self.__dim_list

    @dim_list.setter
    def dim_list(self, val):
        dl = self.__set_spec(val)
        if dl.shape != self.shape:
            raise ValueError
        else:
            self.__dim_list = dl.to_numpy()

    @property
    def din(self):
        return self.__din

    @din.setter
    def din(self, val):
        if not self.__check_din(val):
            raise ValueError
        else:
            self.__din = val

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

    @property
    def pss(self):
        return self.__pss

    @pss.setter
    def pss(self, val):
        if not isinstance(val, np.ndarray):
            raise TypeError
        elif not self.__valist([x for xs in val.tolist() for x in xs], \
                               "__PSS"):
            raise TypeError
        elif val.shape != self.shape:
            raise ValueError
        else:
            self.__pss = val








