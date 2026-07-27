#!/bin/bash

#Per importare una particolare lista file, passare la query usata per recuperare la lista file

BASEPATH="/home/acutest/GAS/bin"
BASEPATH_LOG="/home/silvia"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
ANNO=`expr $ANNOS + 0`
MESE=`date -d "$DD" +%m`
GIORNO=`date -d "$DD" +%d`
CURTMS=`date +%Y%m%d%H%M%S`
FLUSSO="SAG1"
LOG_FILE="$BASEPATH_LOG""/schedulazioni/log_crontab/log_${FLUSSO}_GAS__$CURTMS"".txt"

dd=`date`
echo $>> ${LOG_FILE}
echo "*****************************************************************************************************" $>> ${LOG_FILE}
echo "Start" $>> ${LOG_FILE}
cd ${BASEPATH} > /dev/null

./run_sag.sh $>> ${LOG_FILE}

echo "Caricamento file" $>> ${LOG_FILE} 
./SAG1_debug.sh -i ${BASEPATH} -p 1 $* $>> ${LOG_FILE}

echo "Start Verifiche SQL" $>> ${LOG_FILE}
hive -f sag/Table_HIVE_SAG1_V3_rec.sql $>> ${LOG_FILE}

echo "End" $>> ${LOG_FILE}
echo "*****************************************************************************************************" $>> ${LOG_FILE}
cd - > /dev/null
