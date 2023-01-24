import datetime as dt
from MatrixColumn import MatrixColumn
import pandas as pd
import numpy as np


class UAVObjectBase(object):
    """
    Object to store data from the flight controller sensors during measurement period.

    :param data_length: The row length of the data
    :type data_length: int
    :param time: Time column (s)
    :type time: pd.DatetimeIndex
    :param press_hpa: Pressure column (hPa)
    :type press_hpa: np.matrix
    :param long: GPS Longitude (dec)
    :type long: np.matrix
    :param lat: GPS Latitude (dec)
    :type lat: np.matrix
    :param gps_alt_m: GPS Altitude (m)
    :type gps_alt_m: np.matrix
    :param pitch_deg: Pitch (deg)
    :type pitch_deg: np.matrix
    :param roll_deg: Roll (deg)
    :type roll_deg: np.matrix
    :param yaw_deg: Yaw (deg)
    :type yaw_deg: np.matrix
    :param asp_ms: Airspeed from pitot tube (m/s)
    :type asp_ms: np.matrix
    """
    def __init__(self, data_length, date_time, time, press_hpa,
                 long, lat, gps_alt_m,
                 pitch_deg, roll_deg, yaw_deg,
                 asp_ms):

        self._data_length = None
        self._date_time = None

        self._time = None
        self._press_hpa = None
        self._long = None
        self._lat = None
        self._gps_alt_m = None
        self._pitch_deg = None
        self._roll_deg = None
        self._yaw_deg = None
        self._asp_ms = None

        self.data_length = data_length
        self.date_time = date_time

        self.time = time
        self.press_hpa = self._check_row_length(press_hpa, self.data_length)
        self.long = self._check_row_length(long, self.data_length)
        self.lat = self._check_row_length(lat, self.data_length)
        self.gps_alt_m = self._check_row_length(gps_alt_m, self.data_length)
        self.pitch_deg = self._check_row_length(pitch_deg, self.data_length)
        self.roll_deg = self._check_row_length(roll_deg, self.data_length)
        self.yaw_deg = self._check_row_length(yaw_deg, self.data_length)
        self.asp_ms = self._check_row_length(asp_ms, self.data_length)

    press_hpa = MatrixColumn("press_hpa", 1)
    long = MatrixColumn("long", 1)
    lat = MatrixColumn("lat", 1)
    gps_alt_m = MatrixColumn("gps_alt_m", 1)
    pitch_deg = MatrixColumn("pitch_deg", 1)
    roll_deg = MatrixColumn("roll_deg", 1)
    yaw_deg = MatrixColumn("yaw_deg", 1)
    asp_ms = MatrixColumn("asp_ms", 1)

    def to_dataframe(self):
        """
        Converts the data structure to a pandas dataframe

        :return: DataFrame with time index column
        :rtype: pd.DataFrame
        """
        param_list = [self.lat, self.long, self.gps_alt_m,
                      self.pitch_deg, self.roll_deg, self.yaw_deg,
                      self.asp_ms, self.press_hpa]
        headers = ['Latitude', 'Longitude', 'Altitude',
                   'Pitch (Deg)', 'Roll (Deg)', 'Yaw (Deg)',
                   'Airspeed (m/s)', 'Pressure (hPa)']
        df = param_list[0]
        for i in range(len(param_list) - 1):
            df = np.concatenate((df, param_list[i + 1]), axis=1)
        return pd.DataFrame(data=df, index=self.time, columns=headers)

    @staticmethod
    def _check_row_length(val, row_length):
        """
        A function to ensure a value has the required row length to be assigned to a '_matrix_column' object.

        :param val: The data in a matrix column.
        :type val: np.matrix
        :param row_length: The required length of row.
        :type row_length: int

        :raises ValueError: if the length of the matrix column does not match the specified 'row_length'

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
            raise ValueError('Time must have the same array length as the matrix columns')
        else:
            self._time = val

    @property
    def data_length(self):
        """The numeric length of the columns in number of cells"""
        return self._data_length

    @data_length.setter
    def data_length(self, val):
        if isinstance(val, int):
            self._data_length = val
        else:
            raise TypeError('Value must be in integer format')

    @property
    def date_time(self):
        """A python date_time variable to describe the time and date of the beginning of recording"""
        return self._date_time

    @date_time.setter
    def date_time(self, val):
        if isinstance(val, dt.datetime):
            self._date_time = val
        else:
            raise TypeError('Value must be in python date_time format')
