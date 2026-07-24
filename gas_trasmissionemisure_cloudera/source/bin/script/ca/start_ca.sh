#!/bin/bash
TEMP=`date +%Y%m%d`
PATH_HOME=/home/acutest/GAS
PATH_LOG=$PATH_HOME/logs/
PATH_BIN=$PATH_HOME/bin/script_sql/ca

LOGFILE=file_$TEMP.log
CURRENT_DATE=`date +%Y%m%d%H%M%S`

echo `date +"%y%m%d %T"` + " Inizio calcolo CA" >> $LOGFILE
hive -f $PATH_BIN/CA5.sql -hivevar YEAR_VAR=2020 -hivevar CURRENT_DATE=${CURRENT_DATE} >> $LOGFILE

echo `date +"%y%m%d %T"` + " Fine calcolo CA" >> $LOGFILE

#DT =`date +%Y%m`
#DT=`hive -e 'show partitions au.TAB_DATI_SETTLE_SAG_RES_ORACLE;' 2> /dev/null | sed '$d' | sed '$d' | sort | tail -n 1`
#IFS='='; arrIN=($PARTITION); unset IFS;
echo `date +"%y%m%d %T"` + " Inizio Scrittura" >> $LOGFILE
#./scrittura_oracle.sh ${arrIN[1]}
#./scrittura_oracle.sh ${DT}
./scrittura_oracle.sh 
echo `date +"%y%m%d %T"` + " Fine Scrittura" >> $LOGFILE

