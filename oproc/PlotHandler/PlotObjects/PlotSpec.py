import pandas as pd
import numpy as np
from pydoc import locate


class PSS(object):
    def __init__(self, pssd: dict):
        self.data = None
        self.n_lines = None
        self.n_dims = None
        [setattr(self, k, v) for k, v in pssd.items() if k in self.__dict__]
        self.data = np.asarray(self.data)
        self.__check()

    def __check(self):
        for k, v in self.__dict__.items():
            if v is None:
                raise AttributeError('%s is not set' % k)


class PlotSpec(object):
    def __init__(self, nrows: int, ncols: int, din: list[list[str]], \
                plot_spec: np.ndarray | list[list[int]] | None | int = None, \
                dim_list: np.ndarray | list[list[int]] | None | int = None):

        self.__shape = None
        self.__plot_spec = None
        self.__dim_list = None
        self.__din = None
        self.__pss = None

        self.shape = (nrows, ncols)
        self.dim_list = dim_list
        self.plot_spec = plot_spec
        self.din = din
        self.pss = self.__make_pss()

        if (self.dim_list.shape != self.shape) or \
           (self.plot_spec.shape != self.shape):
            raise ValueError

    def __get__(self):
        return self.pss

    def __len__(self):
        return np.prod(self.shape)

    def __make_pss(self) -> np.ndarray:

        def __make_pss_entry(din, dim, ndat) -> dict:
            return {"data": din, "n_dims": dim, "n_lines": ndat}

        pss = []
        index = 0
        for row in range(self.shape[0]):
            pssrw = []
            for col in range(self.shape[1]):
                pss_inst = PSS(__make_pss_entry(self.din[index],\
                                            self.dim_list[row, col],\
                                            self.plot_spec[row, col]))
                pssrw.append(pss_inst)
                index += 1
            pss.append(pssrw)
        return np.array(pss)

    @staticmethod
    def __blank_df(nrows, ncols, val) -> pd.DataFrame:
        return pd.DataFrame(np.ones([nrows, ncols], int) * val)

    @staticmethod
    def __valist(test: list, tp: str) -> bool:
        if isinstance(tp, str):
            return all(isinstance(sub, locate(tp)) for sub in test)
        else:
            return all(isinstance(sub, tp) for sub in test)

    def __check_din(self, val) -> bool:
        if len(val) != np.prod(self.shape):
            return False
        # dims = pd.DataFrame(self.dim_list).to_dict(orient='tight')['data']
        # dims = [x for xs in dims for x in xs]
        dims = self.dim_list
        for v, i in zip(val, range(np.prod(self.shape))):
            d = self.dim_list[self.plot_coords(i)]
            n = self.plot_spec[self.plot_coords(i)]
            if not isinstance(v, np.ndarray):
                return False
            elif v.shape[1] != d:
                return False
            elif v.shape[0] != n:
                return False
            else:
                continue
        return True

    def __set_spec(self, val, base_type="int"):
        if isinstance(val, list):
            tval = val # It's like this for legacy reasons, don't @ me
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

    def plot_coords(self, number: int):
        coords = (int(np.floor(number/self.shape[1])), number % self.shape[1])
        if coords[0] > self.shape[0]:
            raise ValueError("number exceeds max plots")
        else:
            return coords

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
            self.__din = np.asarray(val)

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
        elif not self.__valist([x for xs in val.tolist() for x in xs], PSS):
            raise TypeError
        elif val.shape != self.shape:
            raise ValueError
        else:
            self.__pss = val

    @property
    def ivars(self):
        x = self.din
        return np.unique(np.asarray(x).flatten())





