#!/bin/bash
BASEPATH_ROOT="/mnt/isilonshare1/Software/GAS"

TEMP=`date +%Y%m%d`
PATH_LOG=BASEPATH_ROOT/log/
PATH_BIN=BASEPATH_ROOT/bin/bonifica

LOGFILE=file_$TEMP.log


echo `date +"%y%m%d %T"` + " Inizio bonifica file" >> $LOGFILE

python $PATH_BIN/bonifica_C1_C2.py 

echo `date +"%y%m%d %T"` + " Fine bonifica file" >> $LOGFILE
