#!/bin/sh
# Assuming you are working in the Scripts directory

dts=${!#}
SCRIPT_PATH="$PWD/../UCASSData"
DEBUG=0

while getopts ":d" option;
do
  case $option in
    d) let DEBUG=1;;
  esac
done

echo "Debug is $DEBUG"
export PYTHONPATH="$PWD/.."

now=$(date +%s)
lfn="ImportLog_$now.log"
log=$(realpath "Logs/$lfn")

echo "Starting import process for datetimes: $dts"
cd $SCRIPT_PATH
if [ $DEBUG = 1 ] ; then
  python -m pdb csv_import_generic.py "$dts" 2>&1 | tee $log
else
  python csv_import_generic.py "$dts" 2>&1 | tee $log
fi
