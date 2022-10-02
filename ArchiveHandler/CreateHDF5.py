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
                 glitch, ltof, rejrat, time, data_length):

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
    def counts(self):
        """counts: 16 by x array of raw count data, where x is the dataset column length."""
        return self._counts

    @counts.setter
    def counts(self, value):
        if isinstance(val, np.matrix):
            self._counts = val
        else:
            raise TypeError('Value must be in numpy matrix format')

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


class MetaDataBase(object):
    def __init__(self, description, datetime, gcs_dd):
        self._gcs_dd = None
        self._description = None
        self._datetime = None
        self.description = description
        self.datetime = datetime
        self._start_epoch_ms = (self.datetime - dt.datetime.utcfromtimestamp(0)).total_seconds() * 1000
        self.gcs_dd = gcs_dd

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


