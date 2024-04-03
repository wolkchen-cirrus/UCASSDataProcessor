import click
import subprocess

@click.group()
def cli():
    return

@cli.command()
@click.argument('dts')
@click.option('-f', '--h5-file', default=None)
def dimp(h5_file, dts):
    ip = subprocess.Popen(['python', 'csv_import_generic.py',
                           '-f', h5_file, dts],
                          stdout=subprocess.PIPE)
    print(ip)

if __name__ == '__main__':
    cli()

