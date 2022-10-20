"""
==============
CreateHDF5.py
==============
This script turns .csv data with columns defined by the user into a standard HDF5 file.
"""

import os
import h5py as h5
import ConfigHandler as ch
import datetime as dt
import pandas as pd
import numpy as np
import gc


class _MatrixColumn(object):
    """_MatrixColumn: A class designed for data input protection of the main stratified variables, for example counts,
    time etc."""
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
            raise TypeError('Value must be of type: integer')


def _get_ucass_calibration(serial_number):
    cali_path = os.path.join(ch.read_config_key('ucass_calibration_path'), serial_number)
    cal_file = None
    for filename in os.listdir(cali_path):
        if 'CalData' in filename:
            cal_file = filename
            break
    if not cal_file:
        raise FileNotFoundError("Calibration file does not exist")
    cali_path = os.path.join(cali_path, cal_file)
    with open(cali_path) as cf:
        data = cf.readlines()
    gain = None
    sl = None
    for line in data:
        if 'Gain' in line:
            gain = float(line.split('=')[-1])
        elif 'SL' in line:
            sl = float(line.split('=')[-1])
    if (not gain) or (not sl):
        raise AttributeError("Either gain or sl not found in file")
    else:
        return gain, sl


def csv_import_user(csv_path):
    flight_date = input('Enter Flight Date yyyy/mm/dd:')
    h5_path = ch.read_config_key('base_data_path')
    ucass_df = h5.File('UCASS-DF_', 'w')


def csv_import_fmi2022bme(ucass_csv_path, fc_log_path, bme_log_path):
    serial_number = ucass_csv_path.split('_')[1]
    cali_gain, cali_sl = _get_ucass_calibration(serial_number)
    date_time = pd.to_datetime(ucass_csv_path.split('_')[2]+ucass_csv_path.split('_')[3],
                               format='%Y%m%d_%H%M%S%f') - dt.timedelta(hours=3, minutes=0)
    description = input('Description of data:')
    with open(ucass_csv_path) as df:
        data = df.readlines()
        bbs_adc = data[3].split(',')[:16]
        bin_header_list = data[4].split(',')[1:17]
    df = pd.read_csv(ucass_csv_path, delimiter=',', header=4)
    data_length = df.shape[0]
    time = np.matrix(df['UTC datetime']).T
    counts = np.matrix(df[bin_header_list]).T
    mtof1 = np.matrix(df['Bin1ToF / us']).T
    mtof3 = np.matrix(df['Bin3ToF / us']).T
    mtof5 = np.matrix(df['Bin5ToF / us']).T
    mtof7 = np.matrix(df['Bin7ToF / us']).T
    period = np.matrix(df['period']).T
    csum = np.matrix(df['checksum']).T
    glitch = np.matrix(df['reject_glitch']).T
    ltof = np.matrix(df['reject_longToF']).T
    rejrat = np.matrix(df['reject_ratio']).T

    ucass_va = UCASSVAObjectBase(serial_number, bbs_adc, cali_gain, cali_sl,
                                 counts, mtof1, mtof3, mtof5, mtof7, period, csum, glitch, ltof, rejrat,
                                 time, data_length, description, date_time)


class METObjectBase(object):
    def __init__(self, data_length, time,
                 temp_deg_c, rh, press_hpa):

        self._data_length = None

        self._time = None
        self._temp_dec_c = None
        self._rh = None
        self._press_hpa = None

        self.data_length = data_length

        self.time = time
        self.temp_deg_c = temp_deg_c
        self.rh = rh
        self.press_hpa = press_hpa

        self.check_col_length()

    time = _MatrixColumn("time", 1)
    temp_deg_c = _MatrixColumn("temp_deg_c", 1)
    rh = _MatrixColumn("rh", 1)
    press_hpa = _MatrixColumn("press_hpa", 1)

    @classmethod
    def check_col_length(cls):
        r = cls.data_length
        for obj in gc.get_objects():
            if isinstance(obj, _MatrixColumn):
                if int(obj.shape[0]) == r:
                    pass
                else:
                    raise ValueError("object %s must have length %i" % obj, r)

    @property
    def data_length(self):
        """data_length: the numeric length of the columns in number of cells"""
        return self._data_length

    @data_length.setter
    def data_length(self, val):
        if isinstance(val, int):
            self._data_length = val
        else:
            raise TypeError('Value must be in integer format')


class UAVObjectBase(object):
    def __init__(self, data_length, time, press_hpa,
                 long, lat, gps_alt_m,
                 pitch_deg, roll_deg, yaw_deg,
                 asp_ms):

        self._data_length = None

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

        self.time = time
        self.press_hpa = press_hpa
        self.long = long
        self.lat = lat
        self.gps_alt_m = gps_alt_m
        self.pitch_deg = pitch_deg
        self.roll_deg = roll_deg
        self.yaw_deg = yaw_deg
        self.asp_ms = asp_ms

        self.check_col_length()

    time = _MatrixColumn("time", 1)
    press_hpa = _MatrixColumn("press_hpa", 1)
    lon = _MatrixColumn("lon", 1)
    lat = _MatrixColumn("lat", 1)
    gps_alt_m = _MatrixColumn("gps_alt_m", 1)
    pitch_deg = _MatrixColumn("pitch_deg", 1)
    roll_deg = _MatrixColumn("roll_deg", 1)
    yaw_deg = _MatrixColumn("yaw_deg", 1)
    asp_ms = _MatrixColumn("asp_ms", 1)

    @classmethod
    def check_col_length(cls):
        r = cls.data_length
        for obj in gc.get_objects():
            if isinstance(obj, _MatrixColumn):
                if int(obj.shape[0]) == r:
                    pass
                else:
                    raise ValueError("object %s must have length %i" % obj, r)

    @property
    def data_length(self):
        """data_length: the numeric length of the columns in number of cells"""
        return self._data_length

    @data_length.setter
    def data_length(self, val):
        if isinstance(val, int):
            self._data_length = val
        else:
            raise TypeError('Value must be in integer format')


class UCASSVAObjectBase(object):
    def __init__(self, serial_number, bbs_adc, cali_gain, cali_sl,
                 counts, mtof1, mtof3, mtof5, mtof7, period, csum,
                 glitch, ltof, rejrat, time, data_length,
                 description, date_time):

        self._description = None
        self._date_time = None

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

        self.description = description
        self.date_time = date_time
        self._start_epoch_ms = (self.date_time - dt.datetime.utcfromtimestamp(0)).total_seconds() * 1000

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

        self.check_col_length()

    counts = _MatrixColumn("counts", 16)
    time = _MatrixColumn("time", 1)
    mtof1 = _MatrixColumn("mtof1", 1)
    mtof3 = _MatrixColumn("mtof3", 1)
    mtof5 = _MatrixColumn("mtof5", 1)
    mtof7 = _MatrixColumn("mtof7", 1)
    period = _MatrixColumn("period", 1)
    csum = _MatrixColumn("csum", 1)
    glitch = _MatrixColumn("glitch", 1)
    ltof = _MatrixColumn("ltof", 1)
    rejrat = _MatrixColumn("rejrat", 1)

    @classmethod
    def check_col_length(cls):
        r = cls.data_length
        for obj in gc.get_objects():
            if isinstance(obj, _MatrixColumn):
                if int(obj.shape[0]) == r:
                    pass
                else:
                    raise ValueError("object %s must have length %i" % obj, r)

    @property
    def data_length(self):
        """data_length: the numeric length of the columns in number of cells"""
        return self._data_length

    @data_length.setter
    def data_length(self, val):
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
    def date_time(self):
        """date_time: A python date_time variable to describe the time and date of the beginning of recording"""
        return self._date_time

    @date_time.setter
    def date_time(self, val):
        if isinstance(val, dt.datetime):
            self._date_time = val
        else:
            raise TypeError('Value must be in python date_time format')

    @property
    def start_epoch_ms(self):
        """start_epoch_ms: Time of record start since epoch in milliseconds"""
        return self._start_epoch_ms

    @start_epoch_ms.setter
    def start_epoch_ms(self, val):
        raise PermissionError('Do not re-assign epoch')
