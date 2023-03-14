"""
Library of tools to handle mavlink log data in raw data processing.
"""

import datetime as dt
import pandas as pd
import subprocess
import os.path
from ..ArchiveHandler import ImportLib as im
from ..ArchiveHandler import Utilities as utils
from .. import ConfigHandler as ch
from pymavlink import mavutil
import json
import array
import warnings


# Redefining print function with timestamp
old_print = print


def timestamped_print(*args, **kwargs):
    old_print(f'({dt.now()})', *args, **kwargs)


print = timestamped_print


def read_mavlink_log(log_path: str, message_names: dict) -> dict:
    """
    A function to read a mavlink log, with specified message and data names,
    into arrays. Calls the "mavlogdump.py" file as a subprocess and therefore
    takes a long time to run; function depreciated in future versions, in
    favour of json.

    :param log_path: The path to the mavlink '.log' file
    :param message_names: Specification of message names

    :return: The synchronised and resampled data frame.
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

    warnings.warn('Feature will be discontinued in future versions',
                  category=DeprecationWarning)

    mav_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            '../mavlogdump.py')
    proc = subprocess.Popen(['python', mav_path, "--types",
                             ','.join(message_names.keys()), log_path],
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
        fc_dict[mess] = pd.DataFrame(fc_list, columns=message_names[mess],
                                     index=fc_time)

    df = im.sync_and_resample(list(fc_dict.values()), '0.1S')

    return im.df_to_dict(df)


def read_json_log(log_path: str, message_names: dict) -> dict:
    """
    A function to read a mavlink log, with specified message and data names,
    into arrays.

    :param log_path: The path to the json file
    :param message_names: Specification of message

    :return: The synchronised and resampled data frame
    """
    # Load JSON
    log_path = utils.get_log_path(log_path, 'FC Proc')
    with open(log_path, 'r') as f:
        log_dict = json.load(f)
    fc_dict = {}
    for m in message_names:
        # Get message names with listcomp!
        data = [x for x in log_dict if m == x['meta']['type']]
        # Merge meta and data, I love listcomp!
        data = [x['data'] | {'timestamp': x['meta']['timestamp']}
                for x in data]
        # Timestoomp --> dootime
        data = [x | {'timestamp': dt.datetime.fromtimestamp(x['timestamp'])}
                for x in data]
        # Format as dataframe with columns, and a timestamp index for syncing
        fc_dict[m] = pd.DataFrame(data).set_index('timestamp')
        # Remove unwanted data
        rm_list = list(fc_dict[m].columns.values)
        [rm_list.remove(x) for x in message_names[m]]
        fc_dict[m] = fc_dict[m].drop(rm_list, axis=1)

    df = im.sync_and_resample(list(fc_dict.values()), '0.1S')

    return im.df_to_dict(df)


def log_to_json(fc_log: str, in_dir: str = 'FC', out_dir: str = 'FC Proc'):

    # Get path for outputs
    base = utils.get_log_path(None, out_dir)

    # Create path if it does not exist
    if not os.path.exists(base):
        print('Creating data directory structure at base path %s'
              % ch.getval('base_data_path'))
        utils.make_dir_structure()

    # Convert input path to list if it is not
    fc_log = im.to_list(fc_log)

    # Loop through input logs
    for log in fc_log:

        # Convert to abs path
        log = utils.get_log_path(log, in_dir)
        out_file = os.path.join(base, os.path.split(log)[-1]
                                .replace('.log', '.json'))
        mlog = mavutil.mavlink_connection(log, robust_parsing=True,
                                          dialect='ardupilotmega')

        db = []
        exclude = ['BAD_DATA', 'FMT', 'PARM', 'MULT', 'FMTU']
        # Main proc loop
        while True:
            # Get mavlink log line
            m = mlog.recv_match()
            # Break if empty; the logging is complete
            if m is None:
                break
            # Get message type for later
            m_type = m.get_type()
            # Skip over bad data
            if m_type in exclude:
                continue
            # Grab the timestamp.
            timestamp = getattr(m, '_timestamp', 0.0)
            # Format our message as a Python dict
            data = m.to_dict()
            # Remove the mavpackettype value as we specify that later.
            del data['mavpackettype']
            # Prepare the message as a single object
            meta = {"type": m_type, "timestamp": timestamp}
            # convert any array.array into lists:
            for key in data.keys():
                if isinstance(data[key], array.array):
                    data[key] = list(data[key])
            # convert any byte-strings into utf-8 strings. Don't die trying.
            for key in data.keys():
                if isinstance(data[key], bytes):
                    data[key] = im.to_string(data[key])
            db.append({"meta": meta, "data": data})

        with open(out_file, mode='a') as out_stream:
            out_stream.write(json.dumps(db))

        print('Created %s' % out_file)
