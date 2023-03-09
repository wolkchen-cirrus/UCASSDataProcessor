#!/bin/sh
# Assuming you are working in the Scripts directory

SHAREDIR=$(realpath "ShareInfo")
MODDIR=$(realpath "../UCASSData")

rm -rf $SHAREDIR/UCASSConfig*

cp $MODDIR/ConfigHandler/UCASSConfig.json $SHAREDIR/UCASSConfigExample.json
