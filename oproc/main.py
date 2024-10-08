import click
import io
import time
import sys
import os
import subprocess

import oproc
import oproc.ConfigHandler as ch
from oproc.ArchiveHandler.HDF5DataObjects.CampaignFile import CampaignFile
from oproc.ArchiveHandler.HDF5DataObjects.H5dd import H5dd
#from oproc.ArchiveHandler import ImportLib as im
from oproc.ArchiveHandler import Utilities as utils
from oproc.ProcHandler.ProcObjects.CalibrateOPC import CalibrateOPC
from oproc.ProcHandler.ProcObjects.NConc import NConc
from oproc.ProcHandler.ProcObjects.MConc import MConc
from oproc.ProcHandler.ProcObjects.SampleVolume import SampleVolume
from oproc.ProcHandler.ProcObjects.ProfileSplit import ProfileSplit
from oproc.ProcHandler.ProcObjects.AirspeedMask import AirspeedMask
from oproc.ProcHandler.ProcObjects.AddMaterial import AddMaterial
from oproc.ProcHandler.ProcObjects.AddWindDat import AddWindDat
from oproc.ProcHandler.ProcObjects.BinCentres import BinCentres
from oproc.ProcHandler.ProcObjects.BinRadii import BinRadii
from oproc.ProcHandler.ProcObjects.EffectiveRadius import EffectiveRadius
from oproc.ProcHandler.ProcObjects.AOAMask import AOAMask
from oproc.ProcHandler.ProcObjects.AirspeedCorrection import AirspeedCorrection
from oproc import newprint

# Redefining print function with timestamp
print = newprint()


os.environ["WORKING_INSTRUMENT"] = "UCASS"
os.environ["AIRSPEED_TYPE"] = "corrected"
os.environ["WORKING_MATERIAL"] = "water"
os.environ["DEFAULT_ISS"] = "pace2022_iss.json"
os.environ["MATERIAL_ISS"] = "ucass_scs_iss.json"
os.environ["WIND_ISS"] = "sammal_wd_iss.json"
os.environ["PLOT_STYLE"] = "CopernicusPlots"


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
def list():
    list = utils.match_raw_files(['UCASS','Met','FC Proc'])
    print(list)

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
        md_list = dd.md
    h5 = H5dd(None)
    for md in md_list:
        cobj = CalibrateOPC(md)
        do = cobj.proc()
        wdobj = AddWindDat(do)
        do = wdobj.proc()
        ascobj = AirspeedCorrection(do)
        do = ascobj.proc()
        svobj = SampleVolume(do)
        do = svobj.proc()
        ncobj = NConc(do)
        do = ncobj.proc()
        psobj = ProfileSplit(do)
        do = psobj.proc()
        aoaobj = AOAMask(do)
        do = aoaobj.proc()
        vzobj = AirspeedMask(do)
        do = vzobj.proc()
        matobj = AddMaterial(do)
        do = matobj.proc()
        bcsobj = BinCentres(do)
        do = bcsobj.proc()
        brobj = BinRadii(do)
        do = brobj.proc()
        mcobj = MConc(do)
        do = mcobj.proc()
        erobj = EffectiveRadius(do)
        do = erobj.proc()

        h5 = h5 + H5dd(do)

    with CampaignFile(h5_path, mode="w") as cf:
        print(md_list)
        cf.write(h5)

@cli.command()
@click.argument('iss')
def isswrite(iss):
    m_path = os.path.dirname(oproc.__file__)
    args = ['python', f"{m_path}/write_iss.py", iss]
    _ = run_subprocess(args)

if __name__ == '__main__':
    cli()

