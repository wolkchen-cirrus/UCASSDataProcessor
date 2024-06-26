import click
import io
import time
import sys
import os
import subprocess

import oproc
import oproc.ConfigHandler as ch
from oproc.ArchiveHandler.HDF5DataObjects.CampaignFile import CampaignFile
from oproc.ProcHandler.ProcObjects.CalibrateOPC import CalibrateOPC
from oproc import newprint

# Redefining print function with timestamp
print = newprint()

def run_subprocess(args):
    filename = ch.getval("log_path")
    with open(filename, "wb") as f:
        process = subprocess.Popen(args,
                                   stderr=subprocess.STDOUT,
                                   stdout=subprocess.PIPE)
        for c in iter(process.stdout.readline, b""):
            sys.stdout.write(c.decode(sys.stdout.encoding))
            f.write(c)
            if process.poll() is None:
                continue
            else:
                break
    return process

@click.group()
def cli():
    return

@cli.command()
def test():
    print("here")

@cli.command()
@click.argument('dts')
@click.option('-f', '--h5-file', default=None)
def rdport(h5_file, dts):
    m_path = os.path.dirname(oproc.__file__)
    args = ['python', f"{m_path}/csv_import_generic.py", '-f', h5_file, dts]
    _ = run_subprocess(args)

@cli.command()
@click.argument('h5-path')
def pdport(h5_path):
    with CampaignFile(h5_path) as cf:
        dd = cf.read()
        md = dd.md
    kwargs = {'material': 0, 'instrument': 0}
    cobj = CalibrateOPC(md[0], **kwargs)
    do = cobj.proc()

@cli.command()
@click.argument('iss')
def isswrite(iss):
    m_path = os.path.dirname(oproc.__file__)
    args = ['python', f"{m_path}/write_iss.py", iss]
    _ = run_subprocess(args)

if __name__ == '__main__':
    cli()

