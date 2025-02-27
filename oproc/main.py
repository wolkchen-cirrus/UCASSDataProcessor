import click
import io
import time
import sys
import os
import subprocess
import numpy as np
from matplotlib import pyplot as plt
from tabulate import tabulate

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
from oproc.ProcHandler.ProcObjects.FRSampleVolume import FRSampleVolume
from oproc.ProcHandler.ProcObjects.ProfileSplit import ProfileSplit
from oproc.ProcHandler.ProcObjects.AirspeedMask import AirspeedMask
from oproc.ProcHandler.ProcObjects.AddMaterial import AddMaterial
from oproc.ProcHandler.ProcObjects.AddWindDat import AddWindDat
from oproc.ProcHandler.ProcObjects.BinCentres import BinCentres
from oproc.ProcHandler.ProcObjects.BinRadii import BinRadii
from oproc.ProcHandler.ProcObjects.EffectiveRadius import EffectiveRadius
from oproc.ProcHandler.ProcObjects.AOAMask import AOAMask
from oproc.ProcHandler.ProcObjects.GetPeriod import GetPeriod
from oproc.ProcHandler.ProcObjects.AirspeedCorrection import AirspeedCorrection
from oproc import newprint
from oproc.PlotHandler.PlotObjects.LinePlot2D import LinePlot2D
from oproc.PlotHandler.PlotObjects.OSMTracePlot import OSMTracePlot
from oproc.PlotHandler.PlotObjects.PlotSpec import PlotSpec

# Redefining print function with timestamp
print = newprint()

# Setup EnvVars (will be set in GUI or profile or something)
instrument="UCASS"
if instrument=="UCASS":
    os.environ["WORKING_INSTRUMENT"] = "UCASS"
    os.environ["AIRSPEED_TYPE"] = "normal"
    os.environ["WORKING_MATERIAL"] = "water"
    os.environ["DEFAULT_ISS"] = "pace2022_sht_iss.json"
    os.environ["MATERIAL_ISS"] = "ucass_scs_iss.json"
    os.environ["WIND_ISS"] = "sammal_wd_iss.json"
    os.environ["SV_TYPE"] = "Airspeed"
elif instrument=="CDP":
    os.environ["SV_TYPE"] = "Airspeed"
    os.environ["WORKING_INSTRUMENT"] = "CDP"
    os.environ["WORKING_MATERIAL"] = "water"
    os.environ["DEFAULT_ISS"] = "cdp_ql_iss.json"
    os.environ["MATERIAL_ISS"] = "cdp_ql_scs_iss.json"
elif instrument=="FFSSP":
    os.environ["SV_TYPE"] = "Airspeed"
    os.environ["WORKING_INSTRUMENT"] = "FFSSP"
    os.environ["WORKING_MATERIAL"] = "water"
    os.environ["DEFAULT_ISS"] = "ffssp_ql_iss.json"
    os.environ["MATERIAL_ISS"] = "ffssp_ql_scs_iss.json"
elif instrument=="PCASP":
    os.environ["SV_TYPE"] = "FlowRate"
    os.environ["WORKING_INSTRUMENT"] = "PCASP"
    os.environ["WORKING_MATERIAL"] = "water"
    os.environ["DEFAULT_ISS"] = "pcasp_ql_iss.json"
    os.environ["MATERIAL_ISS"] = "pcasp_ql_scs_iss.json"
else:
    raise RuntimeError
os.environ["PLOT_STYLE"] = "CopernicusStyle"

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
@click.option('-t', '--match-type', default=None)
def list(match_type):
    if not match_type:
        list = utils.match_raw_files(
#            ['UCASS','Met','SHT','FC Proc','CDP','PCASP'])
            ['UCASS','Met','FC Proc'])
    else:
        list = utils.match_raw_files(
#            ['UCASS','Met','SHT','FC Proc','CDP','PCASP'],
            ['UCASS','Met','FC Proc'],
                                     default_type=match_type)
    print('\n'+tabulate(list, headers='keys', tablefmt='psql'))

@cli.command()
@click.argument('dts')
@click.option('-f', '--h5-file', default=None)
def rdport(h5_file, dts):
    m_path = os.path.dirname(oproc.__file__)
    args = ['python', f"{m_path}/csv_import_generic.py", '-f', h5_file, dts]
    _ = run_subprocess(args)

@cli.command()
@click.argument('h5-path')
@click.option('--save/--no-save', default=False)
def soc_ql(h5_path: str, save: bool):
    phs = {}
    with CampaignFile(h5_path) as cf:
        dd = cf.read()
        md_list = dd.md
        ps = PlotSpec(1, 1, [np.matrix(['Time', 'mass_conc'])],
#                             np.matrix(['Time', 'effective_diameter'])],
                     plot_spec=1,
                     dim_list=2)
    for md in md_list:
        phs[md.date_time.strftime(ch.getval('nominalDTformat'))]\
                = LinePlot2D(md, ps)
    if save:
        fig_path = ch.getval('plot_save_path')
        for name, ph in phs.items():
            plt.figure(ph.fig.number)
            plt.savefig(os.path.join(fig_path, name), bbox_inches='tight')
    else:
        pass
    plt.show()

@cli.command()
@click.argument('h5-path')
def soc_ql_proc(h5_path):
    with CampaignFile(h5_path) as cf:
        dd = cf.read()
        md_list = dd.md
    h5 = H5dd(None)
    for md in md_list:
        cobj = CalibrateOPC(md)
        do = cobj.proc()
        pobj = GetPeriod(do)
        do = pobj.proc()
 #       wdobj = AddWindDat(do)
 #       do = wdobj.proc()
 #       ascobj = AirspeedCorrection(do)
 #       do = ascobj.proc()
 #       svobj = SampleVolume(do)
 #       do = svobj.proc()
        svobj = FRSampleVolume(do)
        do = svobj.proc()
 #       ncobj = NConc(do)
 #       do = ncobj.proc()
 #       psobj = ProfileSplit(do)
 #       do = psobj.proc()
 #       aoaobj = AOAMask(do)
 #       do = aoaobj.proc()
 #       vzobj = AirspeedMask(do)
 #       do = vzobj.proc()
        matobj = AddMaterial(do)
        do = matobj.proc()
        bcsobj = BinCentres(do)
        do = bcsobj.proc()
        brobj = BinRadii(do)
        do = brobj.proc()
#        mcobj = MConc(do)
#        do = mcobj.proc()
#        erobj = EffectiveRadius(do)
#        do = erobj.proc()

        h5 = h5 + H5dd(do)

    with CampaignFile(h5_path, mode="w") as cf:
        print(md_list)
        cf.write(h5)

@cli.command()
@click.argument('h5-path')
@click.option('--save/--no-save', default=False)
def plot(h5_path: str, save: bool):
    plot_args = {'mask': ['PMask1']}
    phs = {}
    with CampaignFile(h5_path) as cf:
        dd = cf.read()
        md_list = dd.md
        ps = PlotSpec(1, 2, [np.matrix(['effective_radius', 'Alt']),
                             np.matrix(['number_conc', 'Alt'])],
                     plot_spec=1,
                     dim_list=2)
    for md in md_list:
        phs[md.date_time.strftime(ch.getval('nominalDTformat'))]\
                = LinePlot2D(md, ps, **plot_args)
    if save:
        fig_path = ch.getval('plot_save_path')
        for name, ph in phs.items():
            plt.figure(ph.fig.number)
            plt.savefig(os.path.join(fig_path, name), bbox_inches='tight')
    else:
        pass
    plt.show()

@cli.command()
@click.argument('h5-path')
@click.option('--save/--no-save', default=False)
def map_plot(h5_path: str, save: bool):
    plot_args = {'Zoom': 16, 'Extent': [24.09, 24.19, 68, 68.03], 'mask':['PMask1']}
    phs = {}
    with CampaignFile(h5_path) as cf:
        dd = cf.read()
        md_list = dd.md
        ps = PlotSpec(1, 1, [np.matrix(['Lng', 'Lat'])],
                     plot_spec=1,
                     dim_list=2)
    for md in md_list:
        phs[md.date_time.strftime(ch.getval('nominalDTformat'))]\
                = OSMTracePlot(md, ps, **plot_args)
    if save:
        fig_path = ch.getval('plot_save_path')
        for name, ph in phs.items():
            name = name+'.pdf'
            plt.figure(ph.fig.number)
            plt.savefig(os.path.join(fig_path, name), bbox_inches='tight', format='pdf')
    else:
        pass
    plt.show()

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

