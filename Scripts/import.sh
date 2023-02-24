#!/bin/sh
# Assuming you are working in the Scripts directory

dts=$1

export PYTHONPATH="$PWD/.."

now=$(date +%s)
lfn="ImportLog_$now.log"
log="Logs/$lfn"
python csv_import_generic.py "$dts" 2>&1 | tee $log
