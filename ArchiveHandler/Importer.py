"""
This script turns .csv data with columns defined by the user into a standard HDF5 file.
"""

import os
import h5py as h5
import ConfigHandler as ch
import datetime as dt
import pandas as pd
import numpy as np
import subprocess


class _MatrixColumn(object):
    """
    A class designed for data input protection of the main stratified variables, for example counts, time etc.

    :param name: The variable name for the data to be stored in, to be preceded by '_' in the calling space.
    :type name: str
    :param c: The number of columns the data require, this is checked upon instantiation.
    :type c: int

    :return: The value assigned to the variable '_name' in the instantiating space.
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


def _get_ucass_calibration(serial_number):
    """
    A function to retrieve the calibration coefficients of a UCASS unit, given its serial number.

    :param serial_number: The serial number of the UCASS unit.
    :type serial_number: str

    :return: gain (float) and sl (float), the calibration coefficients.
    :rtype: tuple
    """
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
    """
    A function to synchronise a number of pandas data frames, then resample with a given time period.

    :param df_list: A list of pandas data frames to be synchronised.
    :type df_list: list
    :param period_str: The time period for resampling, specified as a string, e.g. '0.1S'.
    :type period_str: str

    :return: The synchronised and resampled data frame.
    :rtype: pd.DataFrame
    """
    df = df_list[0]
    for i in range(len(df_list)-1):
        df = pd.concat(df.align(df_list[i+1]), axis='columns')
    return df.dropna(how='all', axis=0).dropna(how='all', axis=1).resample(period_str).mean().bfill()


def _read_mavlink_log(log_path, message_names):
    """
    A function to read a mavlink log, with specified message and data names, into arrays.

    :param log_path: The path to the mavlink '.log' file
    :type log_path: str
    :param message_names: Specification of message names where message_names['name'] = '[var1, var2, ...]'
    :type message_names: dict

    :return: The synchronised and resampled data frame.
    :rtype: pd.DataFrame
    """
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


def _check_datetime_overlap(datetimes, tol_mins=30):
    """
    Checks if any datetime difference exceeds a specified tolerance

    :param datetimes: List of datetimes to be checked
    :type datetimes: list
    :param tol_mins: Max difference between datetimes in minutes
    :type tol_mins: int

    :raise ValueError: If the difference between any two datetimes specified is larger than the tolerance
    """
    delta = []
    for date in datetimes:
        for i in range(len(datetimes)-1):
            delta.append((date - datetimes[i+1]).total_seconds()/60.0)
    if all(x > tol_mins for x in delta):
        raise ValueError("Time delta exceeds threshold (%i)" % tol_mins)
    else:
        return


def csv_import_fmi2022bme(ucass_csv_path, fc_log_path, bme_log_path):
    """
    <<VALID ONLY FOR DATA COLLECTED IN PALLAS, AUTUMN 2022>>. Function to create all the required classes for data
    validation, synchronisation, and to create and populate the HDF5 file for data storage.

    :param ucass_csv_path: The path to the UCASS '.csv' file
    :type ucass_csv_path: str
    :param fc_log_path: The path to the mavlink '.log' file
    :type fc_log_path: str
    :param bme_log_path: The path to the BME280 '.csv' file
    :type bme_log_path: str

    :return:
    :rtype:
    """
    # Check date and time coincidence.
    _check_datetime_overlap([pd.to_datetime('_'.join([ucass_csv_path.split('_')[-3], ucass_csv_path.split('_')[-2]]),
                                            format='%Y%m%d_%H%M%S%f'),
                             pd.to_datetime('_'.join([fc_log_path.split('_')[-3], fc_log_path.split('_')[-2]]),
                                            format='%Y%m%d_%H%M%S%f'),
                             pd.to_datetime('_'.join([bme_log_path.split('_')[-3], bme_log_path.split('_')[-2]]),
                                            format='%Y%m%d_%H%M%S%f')])

    # UCASS Import.
    serial_number = ucass_csv_path.split('_')[-4]
    cali_gain, cali_sl = _get_ucass_calibration(serial_number)
    date_time = pd.to_datetime('_'.join([ucass_csv_path.split('_')[-3], ucass_csv_path.split('_')[-2]]),
                               format='%Y%m%d_%H%M%S%f')
    description = input('Description of data:')
    with open(ucass_csv_path) as df:
        data = df.readlines()
        bbs_adc = data[3].split(',')[:16]
        bin_header_list = data[4].split(',')[1:17]
    df = pd.read_csv(ucass_csv_path, delimiter=',', header=4)
    data_length = df.shape[0]
    try:
        time = pd.DatetimeIndex([dt.datetime.strptime(x[0], '%d/%m/%Y %H:%M:%S')
                                 for x in np.matrix(df['UTC datetime']).T.tolist()])
    except ValueError:
        time = pd.DatetimeIndex([dt.datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S.%f')
                                 for x in np.matrix(df['UTC datetime']).T.tolist()])
    counts = np.matrix(df[bin_header_list])
    mtof1 = np.matrix(df['Bin1ToF / us']).T / 3.0
    mtof3 = np.matrix(df['Bin3ToF / us']).T / 3.0
    mtof5 = np.matrix(df['Bin5ToF / us']).T / 3.0
    mtof7 = np.matrix(df['Bin7ToF / us']).T / 3.0
    period = np.matrix(df['period']).T / float(32.768 * 1e3)
    csum = np.matrix(df['checksum']).T
    glitch = np.matrix(df['reject_glitch']).T
    ltof = np.matrix(df['reject_longToF']).T
    rejrat = np.matrix(df['reject_ratio']).T
    ucass_va = UCASSVAObjectBase(serial_number, bbs_adc, cali_gain, cali_sl,
                                 counts, mtof1, mtof3, mtof5, mtof7, period, csum, glitch, ltof, rejrat,
                                 time, data_length, description, date_time)

    # Flight controller import
    date_time = pd.to_datetime('_'.join([fc_log_path.split('_')[-3], fc_log_path.split('_')[-2]]),
                               format='%Y%m%d_%H%M%S%f')
    mav_messages = {'ARSP': ['Airspeed'],
                    'ATT': ['Roll', 'Pitch', 'Yaw'],
                    'GPS': ['Lat', 'Lng', 'Alt', 'Spd'],
                    'BARO': ['Press']}
    mav_data = _read_mavlink_log(fc_log_path, mav_messages)
    data_length = len(mav_data['Time'])
    fmi_talon = UAVObjectBase(data_length, date_time, mav_data['Time'], mav_data['Press']/100.0,
                              mav_data['Lng'], mav_data['Lat'], mav_data['Alt'],
                              mav_data['Pitch'], mav_data['Roll'], mav_data['Yaw'],
                              mav_data['Airspeed'])

    # Meteorological sensor import
    date_time = pd.to_datetime('_'.join([bme_log_path.split('_')[-3], bme_log_path.split('_')[-2]]),
                               format='%Y%m%d_%H%M%S%f')
    df = pd.read_csv(bme_log_path, delimiter=',', header=0, names=['Time', 'Temp', 'Press', 'RH']).dropna()
    df['Time'] = pd.to_datetime(df['Time'], format='%d/%m/%Y %H:%M:%S')
    data_length = len(df)
    bme280 = METObjectBase(data_length, date_time, pd.DatetimeIndex(df['Time']), np.matrix(df['Temp']).T,
                           np.matrix(df['RH']).T, np.matrix(df['Press']).T)

    # Make full dataframe to be saved
    df = _sync_and_resample([ucass_va.to_dataframe(), bme280.to_dataframe(), fmi_talon.to_dataframe()], '0.5S')

    return df


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
    def __init__(self, data_length, date_time, time,
                 temp_deg_c, rh, press_hpa):

        self._data_length = None
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

    temp_deg_c = _MatrixColumn("temp_deg_c", 1)
    rh = _MatrixColumn("rh", 1)
    press_hpa = _MatrixColumn("press_hpa", 1)

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

    press_hpa = _MatrixColumn("press_hpa", 1)
    long = _MatrixColumn("long", 1)
    lat = _MatrixColumn("lat", 1)
    gps_alt_m = _MatrixColumn("gps_alt_m", 1)
    pitch_deg = _MatrixColumn("pitch_deg", 1)
    roll_deg = _MatrixColumn("roll_deg", 1)
    yaw_deg = _MatrixColumn("yaw_deg", 1)
    asp_ms = _MatrixColumn("asp_ms", 1)

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


class UCASSVAObjectBase(object):
    """
    Object to store data from the UCASS during measurement period.

    :param data_length: The row length of the data
    :type data_length: int
    :param time: Time column (s)
    :type time: pd.DatetimeIndex
    :param serial_number: UCASS Serial Number
    :type serial_number: str
    :param bbs_adc: UCASS bins as a list of ints (ADC vals)
    :type bbs_adc: list
    :param cali_gain: UCASS calibration gain (gradient)
    :type cali_gain: float
    :param cali_sl: UCASS calibration stray light (offset)
    :type cali_sl: float
    :param counts: UCASS raw counts nx16 matrix
    :type counts: np.matrix
    :param mtof1: UCASS raw time of flight data for bin 1 in Micro Seconds
    :type mtof1: np.matrix
    :param mtof3: UCASS raw time of flight data for bin 3 in Micro Seconds
    :type mtof3: np.matrix
    :param mtof5: UCASS raw time of flight data for bin 5 in Micro Seconds
    :type mtof5: np.matrix
    :param mtof7: UCASS raw time of flight data for bin 7 in Micro Seconds
    :type mtof7: np.matrix
    :param period: UCASS sample period in Seconds
    :type period: np.matrix
    :param csum: UCASS checksum
    :type csum: np.matrix
    :param glitch: UCASS glitch trap counts
    :type glitch: np.matrix
    :param ltof: UCASS long time of flight counts
    :type ltof: np.matrix
    :param rejrat: UCASS particle rejection ratio
    :type rejrat: np.matrix
    :param description: Description of data
    :type description: str
    :param date_time: Date and time of measurement start
    :type date_time: dt.datetime
    """
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

        self.counts = self._check_row_length(counts, self.data_length)
        self.time = time
        self.mtof1 = self._check_row_length(mtof1, self.data_length)
        self.mtof3 = self._check_row_length(mtof3, self.data_length)
        self.mtof5 = self._check_row_length(mtof5, self.data_length)
        self.mtof7 = self._check_row_length(mtof7, self.data_length)
        self.period = self._check_row_length(period, self.data_length)
        self.csum = self._check_row_length(csum, self.data_length)
        self.glitch = self._check_row_length(glitch, self.data_length)
        self.ltof = self._check_row_length(ltof, self.data_length)
        self.rejrat = self._check_row_length(rejrat, self.data_length)

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

    def to_dataframe(self):
        """
        Converts the data structure to a pandas dataframe

        :return: DataFrame with time index column
        :rtype: pd.DataFrame
        """
        param_list = [self.counts, self.period, self.mtof1, self.mtof3, self.mtof5, self.mtof7, self.period,
                      self.csum, self.glitch, self.ltof, self.rejrat]
        headers = ['Counts', 'Period (s)', 'Bin 1 ToF (us)', 'Bin 3 ToF (us)', 'Bin 5 ToF (us)', 'Bin 7 ToF (us)',
                   'Checksum', 'Glitch Counts', 'Long ToF Counts', 'Reject Ratio']
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
    def bin_boundaries_adc(self):
        """The instrument specific upper bin boundaries as ADC values."""
        return self._bin_boundaries_adc

    @bin_boundaries_adc.setter
    def bin_boundaries_adc(self, val):
        if isinstance(val, list):
            self._bin_boundaries_adc = val
        else:
            raise TypeError('Value must be in list format')

    @property
    def ucass_serial_number(self):
        """The serial number of the ucass in format UCASS-<version>-<batch>-<id>"""
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
        """Stray light of UCASS unit from last calibration"""
        return self._cali_sl

    @cali_sl.setter
    def cali_sl(self, val):
        try:
            self._cali_sl = float(val)
        except ValueError:
            raise TypeError('Assigned value must be convertible to a float')

    @property
    def description(self):
        """A string describing the data, with notes concerning acquisition"""
        return self._description

    @description.setter
    def description(self, val):
        if isinstance(val, str):
            self._description = val
        else:
            raise TypeError('Value must be in string format')

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

    @property
    def start_epoch_ms(self):
        """Time of record start since epoch in milliseconds"""
        return self._start_epoch_ms

    @start_epoch_ms.setter
    def start_epoch_ms(self, val):
        raise PermissionError('Do not re-assign epoch')
