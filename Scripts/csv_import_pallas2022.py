"""
<<VALID ONLY FOR DATA COLLECTED IN PALLAS, AUTUMN 2022>>. Creates all the required classes for data validation,
synchronisation, and to create and populate the HDF5 file for data storage. Valid as an example function.
"""

from argparse import ArgumentParser
import UCASSData.ConfigHandler as ch
import UCASSData.ArchiveHandler.ImportLib as im
import UCASSData.ArchiveHandler.MavLib as MavLib
from UCASSData.ArchiveHandler.DataObjects.METObjectBase import METObjectBase
from UCASSData.ArchiveHandler.DataObjects.UAVObjectBase import UAVObjectBase
from UCASSData.ArchiveHandler.DataObjects.UCASSVAObjectBase import UCASSVAObjectBase
import UCASSData.ArchiveHandler.Utilities as utils
import numpy as np
import pandas as pd
import datetime as dt
import warnings


parser = ArgumentParser(description=__doc__)
parser.add_argument("--fc-type", default=None, help=".json or .log as FC data input; will infer if not specified ")
parser.add_argument("ucass_log", metavar="UCASS LOG", help="UCASS log path (csv)")
parser.add_argument("fc_log", metavar="FC LOG", help="FC log path (log or json)")
parser.add_argument("met_log", metavar="MET LOG", help="Met log path (csv)")
args = parser.parse_args()

ucass_csv_path = utils.get_log_path(args.ucass_log, 'UCASS')
bme_log_path = utils.get_log_path(args.met_log, 'Met')

if args.fc_type:
    fc_type = args.fc_type.replace('.', '')
else:
    fc_type = utils.infer_fc_log_type(args.fc_log).replace('.', '')
if fc_type == 'json':
    fc_log_path = utils.get_log_path(args.fc_log, 'FC Proc')
elif fc_type == 'log':
    fc_log_path = utils.get_log_path(args.fc_log, 'FC')
else:
    raise ValueError('%s if invalid FC log type' % args.fc_type)

if __name__ == '__main__':

    # Check date and time coincidence.
    im.check_datetime_overlap([im.fn_datetime(ucass_csv_path), im.fn_datetime(fc_log_path),
                               im.fn_datetime(bme_log_path)])

    # UCASS Import.
    serial_number = ucass_csv_path.split('_')[-4]
    cali_gain, cali_sl = im.get_ucass_calibration(serial_number)
    date_time = im.fn_datetime(ucass_csv_path)
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
    date_time = im.fn_datetime(fc_log_path)
    mav_messages = {'ARSP': ['Airspeed'],
                    'ATT': ['Roll', 'Pitch', 'Yaw'],
                    'GPS': ['Lat', 'Lng', 'Alt', 'Spd'],
                    'BARO': ['Press']}
    if fc_type == 'json':
        mav_data = MavLib.read_json_log(fc_log_path, mav_messages)
    else:
        warnings.warn('Attempting to parse FC .log file, go make a coffee this will take a while :D')
        mav_data = MavLib.read_mavlink_log(fc_log_path, mav_messages)
    data_length = len(mav_data['Time'])
    fmi_talon = UAVObjectBase(data_length, date_time, mav_data['Time'], mav_data['Press']/100.0,
                              mav_data['Lng'], mav_data['Lat'], mav_data['Alt'],
                              mav_data['Pitch'], mav_data['Roll'], mav_data['Yaw'],
                              mav_data['Airspeed'])

    # Meteorological sensor import
    date_time = im.fn_datetime(bme_log_path)
    df = pd.read_csv(bme_log_path, delimiter=',', header=0, names=['Time', 'Temp', 'Press', 'RH']).dropna()
    df['Time'] = pd.to_datetime(df['Time'], format='%d/%m/%Y %H:%M:%S')
    data_length = len(df)
    bme280 = METObjectBase(data_length, date_time, pd.DatetimeIndex(df['Time']), np.matrix(df['Temp']).T,
                           np.matrix(df['RH']).T, np.matrix(df['Press']).T)

    # Make full dataframe to be saved
    df = im.sync_and_resample([ucass_va.to_dataframe(), bme280.to_dataframe().drop('Pressure (hPa)', axis=1),
                               fmi_talon.to_dataframe()], str(ch.getval('timestep'))+'S')
    pass
