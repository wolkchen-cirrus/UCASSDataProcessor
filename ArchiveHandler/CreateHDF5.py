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
import subprocess
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
            raise TypeError('Value must be of type: int')


def _check_row_length(val, row_length):
    if val.shape[0] != row_length:
        raise ValueError('Assigned column %i is not specified length %i' % (val.shape[0], row_length))
    return val


def _get_ucass_calibration(serial_number):
    cali_path = os.path.join(ch.read_config_key('ucass_calibration_path'), serial_number)
    cal_file = None
    for fn in os.listdir(cali_path):
        if 'CalData' in fn:
            cal_file = fn
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


def _sync_and_resample(df_list, period_str):
    df = df_list[0]
    for i in range(len(df_list)-1):
        df = pd.concat(df.align(df_list[i+1]), axis='columns')
    return df.dropna(how='all', axis=0).dropna(how='all', axis=1).resample(period_str).mean().bfill()


def _read_mavlink_log(log_path, message_names):

    def _proc_fc_row(fc_row, params):
        time = dt.datetime.strptime(fc_row[:22], '%Y-%m-%d %H:%M:%S.%f')
        output = []
        fc_row = fc_row.split(',')
        for param in params:
            for fc_part in fc_row:
                label = fc_part.split(':')[0]
                if param.replace(' ', '') == label.replace(' ', ''):
                    output.append(float(fc_part.split(':')[-1]))
                    break
        return time, output

    mav_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mavlogdump.py')
    proc = subprocess.Popen(['python', mav_path, "--types", ','.join(message_names.keys()), log_path],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    fd_out = proc.communicate()[0].decode("utf-8")
    fd_out = fd_out.split('\n')
    fd = {}
    for mess in message_names:
        fd_list = []
        for r in fd_out:
            if mess in r:
                fd_list.append(r)
        fd[mess] = fd_list
    
    fc_dict = {}
    for mess in message_names:
        fc_list = []
        fc_time = []
        for row in fd[mess]:
            fc_time_row, proc_row = _proc_fc_row(row, message_names[mess])
            fc_list.append(proc_row)
            fc_time.append(fc_time_row)
        fc_dict[mess] = pd.DataFrame(fc_list, columns=message_names[mess], index=fc_time)

    df = _sync_and_resample(list(fc_dict.values()), '0.1S')

    fc_dict = df.to_dict(orient='list')
    for key in fc_dict:
        fc_dict[key] = np.matrix(fc_dict[key]).T
    fc_dict['Time'] = df.index

    return fc_dict


def csv_import_user(csv_path):
    flight_date = input('Enter Flight Date yyyy/mm/dd:')
    h5_path = ch.read_config_key('base_data_path')
    ucass_df = h5.File('UCASS-DF_', 'w')


def csv_import_fmi2022bme(ucass_csv_path, fc_log_path, bme_log_path):
    serial_number = ucass_csv_path.split('_')[-4]
    cali_gain, cali_sl = _get_ucass_calibration(serial_number)
    date_time = pd.to_datetime('_'.join([ucass_csv_path.split('_')[-3], ucass_csv_path.split('_')[-2]]),
                               format='%Y%m%d_%H%M%S%f') - dt.timedelta(hours=3, minutes=0)
    description = input('Description of data:')
    with open(ucass_csv_path) as df:
        data = df.readlines()
        bbs_adc = data[3].split(',')[:16]
        bin_header_list = data[4].split(',')[1:17]
    df = pd.read_csv(ucass_csv_path, delimiter=',', header=4)
    data_length = df.shape[0]
    time = pd.DatetimeIndex([dt.datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S.%f')
                             for x in np.matrix(df['UTC datetime']).T.tolist()])
    counts = np.matrix(df[bin_header_list])
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

    mav_messages = {'ARSP': ['Airspeed'],
                    'ATT': ['Roll', 'Pitch', 'Yaw'],
                    'GPS': ['Lat', 'Lng', 'Alt', 'Spd'],
                    'BARO': ['Press']}
    mav_data = _read_mavlink_log(fc_log_path, mav_messages)
    data_length = len(mav_data['Time'])

    fmi_talon = UAVObjectBase(data_length, mav_data['Time'], mav_data['Press']/100.0,
                              mav_data['Lng'], mav_data['Lat'], mav_data['Alt'],
                              mav_data['Pitch'], mav_data['Roll'], mav_data['Yaw'],
                              mav_data['Airspeed'])

    pass


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
        self.temp_deg_c = _check_row_length(temp_deg_c, self.data_length)
        self.rh = _check_row_length(rh, self.data_length)
        self.press_hpa = _check_row_length(press_hpa, self.data_length)

    temp_deg_c = _MatrixColumn("temp_deg_c", 1)
    rh = _MatrixColumn("rh", 1)
    press_hpa = _MatrixColumn("press_hpa", 1)

    @property
    def time(self):
        """time: a pandas DatetimeIndex to be used as a dataframe index"""
        return self._time

    @time.setter
    def time(self, val):
        if not isinstance(val, pd.DatetimeIndex):
            raise TypeError('Time must be pandas DatetimeIndex array')
        if len(val) != self.data_length:
            raise ValueError('Time must have the same array length as the matrix columns')

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
        self.press_hpa = _check_row_length(press_hpa, self.data_length)
        self.long = _check_row_length(long, self.data_length)
        self.lat = _check_row_length(lat, self.data_length)
        self.gps_alt_m = _check_row_length(gps_alt_m, self.data_length)
        self.pitch_deg = _check_row_length(pitch_deg, self.data_length)
        self.roll_deg = _check_row_length(roll_deg, self.data_length)
        self.yaw_deg = _check_row_length(yaw_deg, self.data_length)
        self.asp_ms = _check_row_length(asp_ms, self.data_length)

    press_hpa = _MatrixColumn("press_hpa", 1)
    lon = _MatrixColumn("lon", 1)
    lat = _MatrixColumn("lat", 1)
    gps_alt_m = _MatrixColumn("gps_alt_m", 1)
    pitch_deg = _MatrixColumn("pitch_deg", 1)
    roll_deg = _MatrixColumn("roll_deg", 1)
    yaw_deg = _MatrixColumn("yaw_deg", 1)
    asp_ms = _MatrixColumn("asp_ms", 1)

    @property
    def time(self):
        """time: a pandas DatetimeIndex to be used as a dataframe index"""
        return self._time

    @time.setter
    def time(self, val):
        if not isinstance(val, pd.DatetimeIndex):
            raise TypeError('Time must be pandas DatetimeIndex array')
        if len(val) != self.data_length:
            raise ValueError('Time must have the same array length as the matrix columns')

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

        self.counts = _check_row_length(counts, self.data_length)
        self.time = time
        self.mtof1 = _check_row_length(mtof1, self.data_length)
        self.mtof3 = _check_row_length(mtof3, self.data_length)
        self.mtof5 = _check_row_length(mtof5, self.data_length)
        self.mtof7 = _check_row_length(mtof7, self.data_length)
        self.period = _check_row_length(period, self.data_length)
        self.csum = _check_row_length(csum, self.data_length)
        self.glitch = _check_row_length(glitch, self.data_length)
        self.ltof = _check_row_length(ltof, self.data_length)
        self.rejrat = _check_row_length(rejrat, self.data_length)

    counts = _MatrixColumn("counts", 16)
    mtof1 = _MatrixColumn("mtof1", 1)
    mtof3 = _MatrixColumn("mtof3", 1)
    mtof5 = _MatrixColumn("mtof5", 1)
    mtof7 = _MatrixColumn("mtof7", 1)
    period = _MatrixColumn("period", 1)
    csum = _MatrixColumn("csum", 1)
    glitch = _MatrixColumn("glitch", 1)
    ltof = _MatrixColumn("ltof", 1)
    rejrat = _MatrixColumn("rejrat", 1)

    @property
    def time(self):
        """time: a pandas DatetimeIndex to be used as a dataframe index"""
        return self._time

    @time.setter
    def time(self, val):
        if not isinstance(val, pd.DatetimeIndex):
            raise TypeError('Time must be pandas DatetimeIndex array')
        if len(val) != self.data_length:
            raise ValueError('Time must have the same array length as the matrix columns')

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
