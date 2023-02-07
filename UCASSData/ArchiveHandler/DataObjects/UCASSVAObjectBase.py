import datetime as dt
from .MatrixColumn import MatrixColumn
import pandas as pd
import numpy as np


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
    def __init__(self, date_time: dt.datetime, serial_number: str,
                 bbs_adc: list, cali_gain: float, cali_sl: float,
                 data_length: int, description: str,

                 counts=None, mtof1=None, mtof3=None, mtof5=None,
                 mtof7=None, period=None, csum=None, glitch=None, ltof=None,
                 rejrat=None, time=None):

        self._date_time = None
        self._ucass_serial_number = None
        self._bin_boundaries_adc = None

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
        self._start_epoch = (self.date_time - dt.datetime.utcfromtimestamp(0))\
            .total_seconds()

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

    counts = MatrixColumn("counts", 16)
    mtof1 = MatrixColumn("mtof1", 1)
    mtof3 = MatrixColumn("mtof3", 1)
    mtof5 = MatrixColumn("mtof5", 1)
    mtof7 = MatrixColumn("mtof7", 1)
    period = MatrixColumn("period", 1)
    csum = MatrixColumn("csum", 1)
    glitch = MatrixColumn("glitch", 1)
    ltof = MatrixColumn("ltof", 1)
    rejrat = MatrixColumn("rejrat", 1)

    def to_dataframe(self):
        """
        Converts the data structure to a pandas dataframe

        :return: DataFrame with time index column
        :rtype: pd.DataFrame
        """
        param_list = [self.counts, self.mtof1, self.mtof3, self.mtof5, 
                      self.mtof7, self.period, self.csum, self.glitch, 
                      self.ltof, self.rejrat]
        headers = ['Counts, 1', 'Counts, 2', 'Counts, 3', 'Counts, 4', 
                   'Counts, 5', 'Counts, 6', 'Counts, 7', 'Counts, 8', 
                   'Counts, 9', 'Counts, 10', 'Counts, 11', 'Counts, 12', 
                   'Counts, 13', 'Counts, 14', 'Counts, 15', 'Counts, 16', 
                   'Period (s)', 'Mean ToF (us), 1', 'Mean ToF (us), 3', 
                   'Mean ToF (us), 5', 'Mean ToF (us), 7', 'Checksum', 
                   'Glitch Counts', 'Long ToF Counts', 'Reject Ratio']
        df = param_list[0]
        for i in range(len(param_list) - 1):
            df = np.concatenate((df, param_list[i + 1]), axis=1)
        df = pd.DataFrame(data=df, index=self.time, columns=headers)
        headers = df.columns.str.split(', ', expand=True).values
        df.columns = pd.MultiIndex.from_tuples([('', x[0]) if pd.isnull(x[1]) 
                                                else x for x in headers])
        return df

    @staticmethod
    def _check_row_length(val, row_length):
        """
        A function to ensure a value has the required row length to be assigned
        to a '_matrix_column' object.

        :param val: The data in a matrix column.
        :type val: np.matrix
        :param row_length: The required length of row.
        :type row_length: int

        :raises ValueError: if the wrong lenth is measured

        :return: the assigned array, if the row length is correct.
        :rtype: np.matrix
        """
        if val.shape[0] != row_length:
            raise ValueError('Assigned column %i is not specified length %i' 
                             % (val.shape[0], row_length))
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
        """The serial number of the ucass in format 
        UCASS-<version>-<batch>-<id>"""
        return self._ucass_serial_number

    @ucass_serial_number.setter
    def ucass_serial_number(self, val):
        base_error = 'UCASS serial number must be formatted string ' \
                     'UCASS-<version>-<batch>-<id>'
        if isinstance(val, str):
            split_val = val.split('-')
            try:
                int(split_val[-1])
            except ValueError:
                raise ValueError('%s\n<id> is not int' % base_error)
            if len(split_val) != 4:
                raise ValueError('%s\nString is not 4 part delimited with -'
                                 % base_error)
            elif split_val[0] != 'UCASS':
                raise ValueError('%s\nFirst part of string must be \'UCASS\''
                                 % base_error)
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
    def date_time(self):
        """Date and time at start"""
        return self._date_time

    @date_time.setter
    def date_time(self, val):
        if isinstance(val, dt.datetime):
            self._date_time = val
        else:
            raise TypeError('Value must be in python date_time format')

    @property
    def start_epoch(self):
        """Time of record start since epoch in milliseconds"""
        return self._start_epoch

    @start_epoch.setter
    def start_epoch(self, val):
        raise PermissionError('Do not re-assign epoch')
