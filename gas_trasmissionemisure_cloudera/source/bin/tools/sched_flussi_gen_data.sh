#!/bin/bash
ENABLE_CHECK_FILE=true

BASEPATH="/mnt/isilonshare1/Software/GAS/bin"
BASEPATH_LOG="/mnt/isilonshare1/Software/GAS/log"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
ANNO=$2
MESE=$3
GIORNO=$4
CURTMS=`date +%Y%m%d%H%M%S`
FLUSSO="$1"
LOG_FILE="$BASEPATH_LOG""/log_${FLUSSO}_GAS__$CURTMS"".log"
PATH_WORK="/mnt/isilonshare1/GAS_INJ/isilonshare_gas"
PATH_WORK2="/mnt/isilonshare1/GAS_INJ"
PATH_DECOMRPESSIONE="/mnt/isilonshare1/Software/EE/bin/"
PATH_TOOLS=$BASEPATH"/tools"
dd=`date`

echo "Crea directory logs" 
mkdir -p $BASEPATH_LOG
echo "File log: "${LOG_FILE}

echo $>> ${LOG_FILE}
echo "*****************************************************************************************************" $>> ${LOG_FILE}

echo "Start - Data Elaborazione: $anno-$mese-$giorno - $dd" $>> ${LOG_FILE}

rm /tmp/decompressione.log
touch /tmp/decompressione.log
truncate -s 0 /tmp/decompressione.log

cd ${PATH_DECOMRPESSIONE}
echo "./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}" &>> ${LOG_FILE}
echo "./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}"

./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} &>> /tmp/decompressione.log
timestamp_decompressione=$(cat /tmp/decompressione.log | grep  "*** timestamp di decompressione:" | awk '{print $5" "$6}')

echo "timestamp_decompressione: ${timestamp_decompressione}" >>  ${LOG_FILE}
echo "timestamp_decompressione: ${timestamp_decompressione}"
cat /tmp/decompressione.log >> ${LOG_FILE}
cd - > /dev/null

echo "select filename_folder_dest  FROM cmg_gas.report_decompressione where codice = '000' and nvl(filename_folder_dest, '') != '' and dataelaborazione = '${timestamp_decompressione}'  and filename_folder_dest like '%${FLUSSO}%'" > /tmp/query.sql

echo "Start Query" $>>  ${LOG_FILE}

cat ${PATH_TOOLS}/sql/query_insert_elenco_sched.sql $>> ${LOG_FILE}
echo "hive -hiveconf flusso=${FLUSSO} -f ${PATH_TOOLS}/sql/query_insert_elenco_sched.sql" $>> ${LOG_FILE}
echo "hive -hiveconf flusso=${FLUSSO} -f ${PATH_TOOLS}/sql/query_insert_elenco_sched.sql"
echo "Query inserimento completata" >> ${LOG_FILE}

echo "********** Elaborazione FLUSSO: ${FLUSSO} Data Elaborazione: ${timestamp_decompressione} **************" >> ${LOG_FILE}
echo "*** Elaborazione: "
echo "*** Flusso: "$FLUSSO
echo "*** Anno: "$ANNO" Mese: "$MESE" Giorno: "$GIORNO" -- "${CURTMS} &>> ${LOG_FILE}
echo "*************************************************************************************************************" >> ${LOG_FILE}

echo "${BASEPATH}/ingestion_data_debug.sh -i ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO}"
echo "${BASEPATH}/ingestion_data_debug.sh -i  ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO}" >> ${LOG_FILE}
${BASEPATH}/ingestion_data_debug.sh -i  ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO} &>> ${LOG_FILE}

DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
echo "********** Elaborazione completata il $dd" $>> ${LOG_FILE}
echo "*************************************************************************************************************" >> ${LOG_FILE}

#echo "rm -rf $PATH_WORK/TMG_*" &>> ${LOG_FILE}
#rm -rf ${PATH_WORK}/TMG_*  &>> ${LOG_FILE}
#rm -rf ${PATH_WORK2}/TMG_*  &>> ${LOG_FILE}
