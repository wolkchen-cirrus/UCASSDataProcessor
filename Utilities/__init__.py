import pandas as pd
import datetime as dt


def csv_log_timedelta(filepath, hours, time_format_str, out_time_format_str, time_header, names, header=0):
    df = pd.read_csv(filepath, delimiter=',', header=header, names=names).dropna()
    df[time_header] = pd.to_datetime(df[time_header], format=time_format_str) + dt.timedelta(hours=hours)
    df[time_header] = df[time_header].dt.strftime(out_time_format_str)
    new_start_time = pd.to_datetime('_'.join([filepath.split('\\')[-1].split('_')[-3], filepath.split('\\')[-1].split('_')[-2]]), format='%Y%m%d_%H%M%S%f')
    filepath = '\\'.join(['\\'.join(filepath.split('\\')[:-2]), '_'.join([filepath.split('\\')[-1].split('_')[:-2], new_start_time, filepath.split('\\')[-1].split('_')[-1]])])
    if header == 0:
        pass
    else:
        pass
    return df
