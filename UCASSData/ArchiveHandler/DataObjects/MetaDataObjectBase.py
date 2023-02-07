import datetime as dt
import os.path


class UCASSVAObjectBase(object):
    """
    Object to store data from the UCASS during measurement period.

    :param serial_number: UCASS Serial Number
    :type serial_number: str
    :param bbs_adc: UCASS bins as a list of ints (ADC vals)
    :type bbs_adc: list
    :param cali_gain: UCASS calibration gain (gradient)
    :type cali_gain: float
    :param cali_sl: UCASS calibration stray light (offset)
    :type cali_sl: float
    :param description: Description of data
    :type description: str
    :param date_time: Date and time of measurement start
    :type date_time: dt.datetime
    """
    def __init__(self, date_time: dt.datetime = None,
                 serial_number: str = None, bbs_adc: list = None,
                 cali_gain: float = None, cali_sl: float = None,
                 description: str = None, file_list: list = None):

        self._date_time = None
        self._ucass_serial_number = None
        self._file_list = None

        self.description = description
        self.date_time = date_time
        self._start_epoch = (self.date_time - dt.datetime.utcfromtimestamp(0))\
            .total_seconds()

        self.file_list = file_list
        self.ucass_serial_number = serial_number
        self.bin_boundaries_adc = bbs_adc
        self.cali_gain = cali_gain
        self.cali_sl = cali_sl

    @property
    def file_list(self):
        """list of files which these data originated from"""
        return self._file_list

    @file_list.setter
    def file_list(self, val):
        for v in val:
            if not os.path.exists(v):
                raise FileNotFoundError("specify abs paths")
        self._file_list = val

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
