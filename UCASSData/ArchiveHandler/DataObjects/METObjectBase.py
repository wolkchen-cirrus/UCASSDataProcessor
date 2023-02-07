import datetime as dt
from .MatrixColumn import MatrixColumn
import pandas as pd
import numpy as np


class METObjectBase(object):
    """
    Object to store data from meteorological sensors during measurement period.

    :param data_length: The row length of the data
    :type data_length: int
    :param temp_deg_c: Temperature column (degrees C)
    :type temp_deg_c: np.matrix
    :param time: Time column (s)
    :type time: pd.DatetimeIndex
    :param rh: Relative humidity column (%)
    :type rh: np.matrix
    :param press_hpa: Pressure column (hPa)
    :type press_hpa: np.matrix
    """
    def __init__(self, data_length: int = None, date_time: dt.datetime = None,
                 time=None, temp_deg_c=None, rh=None, press_hpa=None):

        self._date_time = None

        self._time = None
        self._temp_dec_c = None
        self._rh = None
        self._press_hpa = None

        self.data_length = data_length
        self.date_time = date_time

        self.time = time
        self.temp_deg_c = self._check_row_length(temp_deg_c, self.data_length)
        self.rh = self._check_row_length(rh, self.data_length)
        self.press_hpa = self._check_row_length(press_hpa, self.data_length)

    temp_deg_c = MatrixColumn("temp_deg_c", 1)
    rh = MatrixColumn("rh", 1)
    press_hpa = MatrixColumn("press_hpa", 1)

    def to_dataframe(self):
        """
        Converts the data structure to a pandas dataframe

        :return: DataFrame with time index column
        :rtype: pd.DataFrame
        """
        param_list = [self.temp_deg_c, self.rh, self.press_hpa]
        headers = ['Temperature C', 'Relative Humidity', 'Pressure (hPa)']
        df = param_list[0]
        for i in range(len(param_list) - 1):
            df = np.concatenate((df, param_list[i+1]), axis=1)
        return pd.DataFrame(data=df, index=self.time, columns=headers)

    @staticmethod
    def _check_row_length(val, row_length):
        """
        A function to ensure a value has the required row length to be assigned
        to a '_matrix_column' object.

        :param val: The data in a matrix column.
        :type val: np.matrix
        :param row_length: The required length of row.
        :type row_length: int

        :raises ValueError: if invalid length'

        :return: the assigned array, if the row length is correct.
        :rtype: np.matrix
        """
        if val.shape[0] != row_length:
            raise ValueError('Assigned column %i is not specified length %i' % (val.shape[0], row_length))
        return val

    @property
    def time(self):
        """A pandas DatetimeIndex to be used as a dataframe index"""
        return self._time

    @time.setter
    def time(self, val):
        if not isinstance(val, pd.DatetimeIndex):
            raise TypeError('Time must be pandas DatetimeIndex array')
        elif len(val) != self.data_length:
            raise ValueError('Invalid row length')
        else:
            self._time = val

    @property
    def date_time(self):
        """A python date_time variable to describe the time and date of the
        beginning of recording"""
        return self._date_time

    @date_time.setter
    def date_time(self, val):
        if isinstance(val, dt.datetime):
            self._date_time = val
        else:
            raise TypeError('Value must be in python date_time format')
