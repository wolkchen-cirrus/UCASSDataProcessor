#!/bin/sh
# Assuming you are working in the Scripts directory

dts=$1

export PYTHONPATH="$PWD/.."

now=$(date +%F)
lfn="ImportLog_$now.log"
log="Logs/$lfn"
python -m pdb csv_import_generic.py "$dts" 2>&1 | tee $log
