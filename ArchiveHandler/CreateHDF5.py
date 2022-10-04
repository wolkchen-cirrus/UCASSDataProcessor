"""
==============
CreateHDF5.py
==============
This script turns .csv data with columns defined by the user into a standard HDF5 file.
"""

import h5py as h5
import ConfigHandler as ch
import datetime as dt
import pandas as pd
import numpy as np


def user_import_csv_data(csv_path):
    flight_date = input('Enter Flight Date yyyy/mm/dd:')
    h5_path = ch.read_config_key('base_data_path')
    ucass_df = h5.File('UCASS-DF_', 'w')


class UCASSVAObjectBase(object):
    def __init__(self, serial_number, bbs_adc, cali_gain, cali_sl,
                 counts, mtof1, mtof3, mtof5, mtof7, period, csum,
                 glitch, ltof, rejrat, time, data_length,
                 description, datetime, gcs_dd):

        self._gcs_dd = None
        self._description = None
        self._datetime = None
        self.description = description
        self.datetime = datetime
        self._start_epoch_ms = (self.datetime - dt.datetime.utcfromtimestamp(0)).total_seconds() * 1000
        self.gcs_dd = gcs_dd

        self._ucass_serial_number = None
        self._bin_boundaries_adc = None
        self._cali_gain = None
        self._cali_sl = None
        self._data_length = None

        self._time = None
        self._counts = None
        self._mtof1 = None
        self._mtof3 = None
        self._mtof5 = None
        self._mtof7 = None
        self._period = None
        self._csum = None
        self._glitch = None
        self._ltof = None
        self._rejrat = None

        self.ucass_serial_number = serial_number
        self.bin_boundaries_adc = bbs_adc
        self.cali_gain = cali_gain
        self.cali_sl = cali_sl
        self.data_length = data_length

        self.counts = counts
        self.time = time
        self.mtof1 = mtof1
        self.mtof3 = mtof3
        self.mtof5 = mtof5
        self.mtof7 = mtof7
        self.period = period
        self.csum = csum
        self.glitch = glitch
        self.ltof = ltof
        self.rejrat = rejrat

    counts = MatrixColumn("counts", self.data_length, 16)
    time = MatrixColumn("time", self.data_length, 1)
    mtof1 = MatrixColumn("mtof1", self.data_length, 1)
    mtof3 = MatrixColumn("mtof3", self.data_length, 1)
    mtof5 = MatrixColumn("mtof5", self.data_length, 1)
    mtof7 = MatrixColumn("mtof7", self.data_length, 1)
    period = MatrixColumn("period", self.data_length, 1)
    csum = MatrixColumn("csum", self.data_length, 1)
    glitch = MatrixColumn("glitch", self.data_length, 1)
    ltof = MatrixColumn("ltof", self.data_length, 1)
    rejrat = MatrixColumn("rejrat", self.data_length, 1)

    @property
    def data_length(self):
        """data_length: the numeric length of the columns in number of cells"""
        return self._data_length

    @data_length.setter
    def data_length(self, value):
        if isinstance(val, int):
            self._data_length = val
        else:
            raise TypeError('Value must be in integer format')

    @property
    def bin_boundaries_adc(self):
        """bin_boundaries_adc: The instrument specific upper bin boundaries as ADC values."""
        return self._bin_boundaries_adc

    @bin_boundaries_adc.setter
    def bin_boundaries_adc(self, val):
        if isinstance(val, list):
            self._bin_boundaries_adc = val
        else:
            raise TypeError('Value must be in list format')

    @property
    def ucass_serial_number(self):
        """ucass_serial_number: The serial number of the ucass in format UCASS-<version>-<batch>-<id>"""
        return self._ucass_serial_number

    @ucass_serial_number.setter
    def ucass_serial_number(self, val):
        if isinstance(val, str):
            split_val = val.split('-')
            try:
                int(split_val[-1])
            except ValueError:
                raise ValueError('UCASS serial number must be formatted string UCASS-<version>-<batch>-<id>\n'
                                 '<id> is not int')
            if len(split_val) != 4:
                raise ValueError('UCASS serial number must be formatted string UCASS-<version>-<batch>-<id>\n'
                                 'String is not 4 part delimited with \'-\'')
            elif split_val[0] != 'UCASS':
                raise ValueError('UCASS serial number must be formatted string UCASS-<version>-<batch>-<id>\n'
                                 'First part of string must be \'UCASS\'')
            else:
                self._ucass_serial_number = val
        else:
            raise TypeError('Value must be in string format')

    @property
    def ucass_gain_mode(self):
        if self.ucass_serial_number is None:
            raise AttributeError('\"ucass_serial_number\" variable not set')
        else:
            if 'AA' in self.ucass_serial_number.split('-')[1]:
                return 'Aerosol'
            elif 'AD' in self.ucass_serial_number.split('-')[1]:
                return 'Droplet'
            elif 'ASL' in self.ucass_serial_number.split('-')[1]:
                return 'Super Low'
            else:
                return 'Unrecognised Gain'

    @property
    def ucass_batch(self):
        if self.ucass_serial_number is None:
            raise AttributeError('\"ucass_serial_number\" variable not set')
        else:
            return self.ucass_serial_number.split('-')[2]

    @property
    def ucass_id(self):
        if self.ucass_serial_number is None:
            raise AttributeError('\"ucass_serial_number\" variable not set')
        else:
            return int(self.ucass_serial_number.split('-')[-1])

    @property
    def ucass_version(self):
        if self.ucass_serial_number is None:
            raise ValueError('\"ucass_serial_number\" variable not set')
        else:
            return self.ucass_serial_number.split('-')[1]

    @property
    def cali_gain(self):
        """Gain of UCASS unit from last calibration"""
        return self._cali_gain

    @cali_gain.setter
    def cali_gain(self, val):
        try:
            self._cali_gain = float(val)
        except ValueError:
            raise TypeError('Assigned value must be convertible to a float')

    @property
    def cali_sl(self):
        """cali_sl: Stray light of UCASS unit from last calibration"""
        return self._cali_sl

    @cali_sl.setter
    def cali_sl(self, val):
        try:
            self._cali_sl = float(val)
        except ValueError:
            raise TypeError('Assigned value must be convertible to a float')

    @property
    def description(self):
        """description: A string describing the data, with notes concerning acquisition"""
        return self._description

    @description.setter
    def description(self, val):
        if isinstance(val, str):
            self._description = val
        else:
            raise TypeError('Value must be in string format')

    @property
    def datetime(self):
        """datetime: A python datetime variable to describe the time and date of the beginning of recording"""
        return self._datetime

    @datetime.setter
    def datetime(self, val):
        if isinstance(val, dt.datetime):
            self._datetime = val
        else:
            raise TypeError('Value must be in python datetime format')

    @property
    def start_epoch_ms(self):
        """start_epoch_ms: Time of record start since epoch in milliseconds"""
        return self._start_epoch_ms

    @start_epoch_ms.setter
    def start_epoch_ms(self, val):
        raise PermissionError('Do not re-assign epoch')

    @property
    def gcs_dd(self):
        """gcs_dd: Approximate global coordinate system decimal degrees"""
        return self._gcs_dd

    @gcs_dd.setter
    def gcs_dd(self, val):
        if isinstance(val, tuple) and isinstance(val[0], float) and isinstance(val[-1], float):
            pass
        else:
            raise TypeError('value must be tuple of two floats: (lat, long)')


class MatrixColumn(object):
    """MatrixColumn: A class designed for data input protection of the main stratified variables, for example counts,
    time etc."""
    def __init__(self, name, r, c):
        self.name = "_" + str(name)
        self._r = None
        self._c = None

        self.r = r
        self.c = c

    def __get__(self, obj, cls=None):
        return getattr(obj, self.name)

    def __set__(self, obj, value):
        if isinstance(value, np.matrix):
            if (self.r, self.c) == value.shape:
                setattr(obj, self.name, value)
            else:
                raise ValueError('Value must be size %i, %i' % self.r, self.c)
        else:
            raise TypeError('Value must be of type: numpy matrix')

    @property
    def r(self):
        return self._r

    @r.setter
    def r(self, val):
        if isinstance(r, int):
            self._r = val
        else:
            raise TypeError('Value must be of type: integer')

    @property
    def c(self):
        return self._c

    @c.setter
    def c(self, val):
        if isinstance(c, int):
            self._c = val
        else:
            raise TypeError('Value must be of type: integer')
