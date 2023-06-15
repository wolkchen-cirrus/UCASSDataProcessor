import datetime as dt
import os.path
from .. import ImportLib as im
from ... import newprint


# Redefining print function with timestamp
print = newprint()


class MetaDataObject(object):
    """
    Object to store meta data during measurement period; fairly rigid, defines
    minimum metadata

    :param serial_number: UCASS Serial Number
    :param bbs: OPC bins as a list of ints (ADC vals)
    :param cali_coeffs: OPC calibration coefficients in order
    :param description: Description of data
    :param date_time: Date and time of measurement start
    :param sample_area: Sample area of the OPC
    """

    def __init__(self, date_time: dt.datetime = None,
                 serial_number: str = None, bbs: list = None,
                 cali_coeffs: tuple = None, sample_area: float = None,
                 description: str = None, file_list: list = None):

        self._date_time = None
        self._ucass_serial_number = None
        self._file_list = None

        if description is None:
            self.description = input("Description of data: ")
        else:
            self.description = description

        if sample_area is None:
            self.SA = float(input("Sample area (m^2, parsed as float): "))
        else:
            self.SA = sample_area
        self.date_time = date_time
        self._start_epoch = (self.date_time - dt.datetime.utcfromtimestamp(0))\
            .total_seconds()

        self.file_list = file_list
        self.ucass_serial_number = serial_number
        self.bin_boundaries_adc = bbs
        if cali_coeffs is None:
            print("No calibration specifed, looking up from serial number")
            self.cali_coeffs = \
                im.get_ucass_calibration(self.ucass_serial_number)
            print("Calibration found for %s" % self.ucass_serial_number)
        else:
            self.cali_coeffs = cali_coeffs

    def __dict__(self):
        return {"date_time": self.date_time,
                "start_epoch": self.start_epoch,
                "file_list": self.file_list,
                "bbs": self.bin_boundaries_adc,
                "description": self.description,
                "seial_number": self.ucass_serial_number,
                "cali_coeffs": self.cali_coeffs,
                "SA": self.SA}

    @property
    def file_list(self):
        """list of files which these data originated from"""
        return self._file_list

    @file_list.setter
    def file_list(self, val):
        fl = []
        for v in val:
            if not os.path.exists(v):
                raise FileNotFoundError("specify abs paths")
            else:
                fl.append(os.path.split(v)[-1])
        self._file_list = fl

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

    def __repr__(self):
        return f'{self.ucass_serial_number}'
