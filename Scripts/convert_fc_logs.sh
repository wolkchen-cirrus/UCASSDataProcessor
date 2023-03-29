#!/bin/sh
# Assuming you are working in the Scripts directory

INDIR="FC"
OUTDIR="FC Proc"
DEBUG=0
SCRIPT_PATH="$PWD/../oproc"

while getopts ":iod:" option;
do
  case $option in
    i) let INDIR=$OPTARG;;
    o) let OUTDIR=$OPTARG;;
    d) let DEBUG=1;;
    *) echo "Invalid Option $OPTARG" && exit 1;;
  esac
done

export PYTHONPATH="$PWD/.."
now=$(date +%s)
lfn="ConvertLog_$now.log"
log=$(realpath "Logs/$lfn")
echo "Log at: $log"

cd $SCRIPT_PATH
if [ $DEBUG = 1 ] ; then
  python -m pdb log_to_json_all.py -d $INDIR -od $OUTDIR 2>&1 | tee $log
else
  python log_to_json_all.py -d $INDIR -od $OUTDIR 2>&1 | tee $log
fi
