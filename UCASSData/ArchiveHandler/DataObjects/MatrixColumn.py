import numpy as np


class MatrixColumn(object):
    """
    A class designed for data input protection of the main stratified
    variables, for example counts, time etc.

    :param name: variable name, to be preceded by '_' in the calling space.
    :type name: str
    :param c: number of columns, this is checked upon instantiation.
    :type c: int

    :return: value assigned to the variable '_name' in the instantiating space.
    :rtype: np.matrix
    """
    def __init__(self, name, c):
        self.name = "_" + str(name)
        self._c = None
        self.c = c

    def __get__(self, obj, cls=None):
        return getattr(obj, self.name)

    def __set__(self, obj, value):
        if isinstance(value, np.matrix):
            if self.c == value.shape[1]:
                setattr(obj, self.name, value)
            else:
                raise ValueError('Value must be size x, %i' % self.c)
        else:
            raise TypeError('Value must be of type: numpy matrix')

    @property
    def c(self):
        return self._c

    @c.setter
    def c(self, val):
        if isinstance(val, int):
            self._c = val
        else:
            raise TypeError('Value must be of type: int')
