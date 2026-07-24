#!/bin/bash

workspace="/mnt/isilonshare1/GAS_INJ/isilonshare_gas"
echo "Pulizia directory di lavoro: ${workspace}"
rm -rf ${workspace}/*

#Ricerca e Verifica sui file TXT contenuti all'interno della directory /mnt/isilonshare_gas
BASEPATH="/mnt/isilonshare1/Software/GAS/"
BASEPATH_LOG="${BASEPATH}log"
BASEPATH_TOOLS="${BASEPATH}bin/tools/reports"
BASEPATH_REPORT="${BASEPATH}reports/"

CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNO=`date -d "$DD" +%Y`
MESE=`date -d "$DD" +%m`
GIORNO=`date -d "$DD" +%d`

ANNO_C=`date -d "$CURDD" +%Y`
MESE_C=`date -d "$CURDD" +%m`
GIORNO_C=`date -d "$CURDD" +%d`


CURTMS=`date +%Y%m%d%H%M%S`

mkdir -p  ${BASEPATH_REPORT}
report_errors="${BASEPATH_REPORT}report_erros_${CURTMS}.csv"
report_warns="/${BASEPATH_REPORT}report_warns_${CURTMS}.csv"

report_errors_hdfs="/user/hive/warehouse/au.db/misure_gas_au/cmg_gas/report_errors"
LOG_FILE="$BASEPATH_LOG""/log_VERIFICHE_TXT_GAS__$CURTMS"".txt"

echo "Inizio ricerca e verifiche sui file TXT: ${CURDD}" >> ${LOG_FILE}
cd ${BASEPATH_TOOLS} >> ${LOG_FILE}
./recupero_txt.sh ${ANNO} ${MESE} ${GIORNO} >> ${LOG_FILE}

echo "Fine ricerca e verifiche sui file TXT: ${CURDD}" >> ${LOG_FILE}
./genera_report.sh >> ${LOG_FILE}

echo "Controllo errori sui file di logs" >> ${LOG_FILE}
./gas_check_logs_error.sh | grep ${ANNO_C}${MESE_C}${GIORNO_C} | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g" > ${report_errors}

grep -rli "Input path does not exist" ${BASEPATH_LOG}/log_crontab/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g" > ${report_errors}
grep -rli 'Traceback' ${BASEPATH_LOG}/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g" > ${report_errors}
grep -rli "ATTENZIONE" ${BASEPATH_LOG}/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "WARN,"$0",DATE"}' | sed "s|DATE|${CURDD}|g" > ${report_warns}
hadoop fs -mkdir  ${report_errors_hdfs} >> ${LOG_FILE}
hadoop fs -put ${report_errors}  ${report_errors_hdfs} >> ${LOG_FILE}
hadoop fs -put ${report_warns}  ${report_errors_hdfs} >> ${LOG_FILE}
echo "Controllo errori completato" >> ${LOG_FILE}

cd -
