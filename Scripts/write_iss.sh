#!/bin/sh
# Assuming you are working in the Scripts directory

ISS_PATH=""
DEFAULT_PATH=$(realpath "ImportStructSpec.json")
SCRIPT_PATH="$PWD/../UCASSData"

while getopts ":p" option;
do
  case $option in
    p) let ISS_PATH=$(realpath "${OPTARG}");;
    *) echo "Invalid Option $OPTARG" && exit 1;;
  esac
done

if [ -z "$ISS_PATH" ] ; then
  echo "Assuming default ISS path $DEFAULT_PATH"
  ISS_PATH=$DEFAULT_PATH
fi

if [ -d "$ISS_PATH" ] ; then
  echo "Path $ISS_PATH does not exist"
  exit 1
fi

echo "Running assignment script for path $ISS_PATH"
export PYTHONPATH="$PWD/.."
now=$(date +%s)
lfn="IssLog_$now.log"
log=$(realpath "Logs/$lfn")
echo "Log at: $log"

cd $SCRIPT_PATH
python write_iss.py $ISS_PATH 2>&1 | tee $log
