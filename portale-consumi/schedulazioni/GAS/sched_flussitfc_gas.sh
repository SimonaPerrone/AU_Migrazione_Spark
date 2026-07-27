#!/bin/bash


BASEPATH="/home/acutest/20190826_GAS/bin"
BASEPATH_LOG="/home/silvia"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
ANNO=`expr $ANNOS + 0`
MESE=`date -d "$DD" +%m`
CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE="$BASEPATH_LOG""/schedulazioni/log_crontab/log_TFC_GAS__$CURTMS"".txt"

echo ${LOG_FILE}
echo " `date` - run_tfc.sh /mnt/Settlement/TSG/TSG2 $ANNO $MESE " >> "$BASEPATH_LOG/schedulazioni/out_sched.log"

cd "$BASEPATH"

"$BASEPATH"/run_tfc.sh "/mnt/Settlement/TSG/TSG2" $ANNO $MESE &> "$LOG_FILE"

