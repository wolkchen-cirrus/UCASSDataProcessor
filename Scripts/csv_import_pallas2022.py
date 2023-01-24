"""
<<VALID ONLY FOR DATA COLLECTED IN PALLAS, AUTUMN 2022>>. Function to create all the required classes for data
validation, synchronisation, and to create and populate the HDF5 file for data storage.
"""

from argparse import ArgumentParser
import UCASSData.ArchiveHandler.Importer as im
from UCASSData import ConfigHandler as ch
import numpy as np
import pandas as pd
import datetime as dt
import os.path


parser = ArgumentParser(description=__doc__)
parser.add_argument("ucass_log", metavar="UCASS LOG")
parser.add_argument("fc_log", metavar="FC LOG")
parser.add_argument("met_log", metavar="MET LOG")
args = parser.parse_args()


def get_log_path(path, t):
    """
    Returns abs path from log filename and type

    :param path:
    :param t:
    :return:
    """
    base = ch.read_config_key('base_data_path')
    if os.path.isabs(path):
        return path
    else:
        return os.path.join(base, t, os.path.split(path)[-1])


if __name__ == '__main__':

    ucass_csv_path = get_log_path(args.ucass_log, 'UCASS')
    fc_log_path = get_log_path(args.fc_log, 'FC')
    bme_log_path = get_log_path(args.met_log, 'Met')

    # Check date and time coincidence.
    im.check_datetime_overlap([pd.to_datetime('_'.join([ucass_csv_path.split('_')[-3], ucass_csv_path.split('_')[-2]]),
                                              format='%Y%m%d_%H%M%S%f'),
                               pd.to_datetime('_'.join([fc_log_path.split('_')[-3], fc_log_path.split('_')[-2]]),
                                              format='%Y%m%d_%H%M%S%f'),
                               pd.to_datetime('_'.join([bme_log_path.split('_')[-3], bme_log_path.split('_')[-2]]),
                                              format='%Y%m%d_%H%M%S%f')])

    # UCASS Import.
    serial_number = ucass_csv_path.split('_')[-4]
    cali_gain, cali_sl = im.get_ucass_calibration(serial_number)
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
    ucass_va = im.UCASSVAObjectBase(serial_number, bbs_adc, cali_gain, cali_sl,
                                    counts, mtof1, mtof3, mtof5, mtof7, period, csum, glitch, ltof, rejrat,
                                    time, data_length, description, date_time)

    # Flight controller import
    date_time = pd.to_datetime('_'.join([fc_log_path.split('_')[-3], fc_log_path.split('_')[-2]]),
                               format='%Y%m%d_%H%M%S%f')
    mav_messages = {'ARSP': ['Airspeed'],
                    'ATT': ['Roll', 'Pitch', 'Yaw'],
                    'GPS': ['Lat', 'Lng', 'Alt', 'Spd'],
                    'BARO': ['Press']}
    mav_data = im.read_mavlink_log(fc_log_path, mav_messages)
    data_length = len(mav_data['Time'])
    fmi_talon = im.UAVObjectBase(data_length, date_time, mav_data['Time'], mav_data['Press']/100.0,
                                 mav_data['Lng'], mav_data['Lat'], mav_data['Alt'],
                                 mav_data['Pitch'], mav_data['Roll'], mav_data['Yaw'],
                                 mav_data['Airspeed'])

    # Meteorological sensor import
    date_time = pd.to_datetime('_'.join([bme_log_path.split('_')[-3], bme_log_path.split('_')[-2]]),
                               format='%Y%m%d_%H%M%S%f')
    df = pd.read_csv(bme_log_path, delimiter=',', header=0, names=['Time', 'Temp', 'Press', 'RH']).dropna()
    df['Time'] = pd.to_datetime(df['Time'], format='%d/%m/%Y %H:%M:%S')
    data_length = len(df)
    bme280 = im.METObjectBase(data_length, date_time, pd.DatetimeIndex(df['Time']), np.matrix(df['Temp']).T,
                              np.matrix(df['RH']).T, np.matrix(df['Press']).T)

    # Make full dataframe to be saved
    # df = _sync_and_resample([ucass_va.to_dataframe(), bme280.to_dataframe(), fmi_talon.to_dataframe()], '0.5S')
