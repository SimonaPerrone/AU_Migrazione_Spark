#!/bin/bash
ENABLE_CHECK_FILE=true
NO_REDIRECT_TO_ING=false

BASEPATH="/home/acutest/GAS/pyspark2"
BASEPATH_LOG="/home/silvia"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
CURTMS=`date +%Y%m%d%H%M%S`
FLUSSO="$1"
LOG_FILE="$BASEPATH_LOG""/schedulazioni/log_crontab/log_${FLUSSO}_GAS__$CURTMS"".txt"
PATH_WORK="/mnt/isilonshare1/GAS_INJ/isilonshare_gas"
PATH_WORK2="/mnt/isilonshare1/GAS_INJ"
dd=`date`


ANNO=`expr $ANNOS + 0`
MESE=`date -d "$DD" +%m`
GIORNO=`date -d "$DD" +%d`

#TEST=true
TEST=false

echo "FILE DI LOG:" ${LOG_FILE}
echo $>> ${LOG_FILE}
echo "*****************************************************************************************************" $>> ${LOG_FILE}

if [ "$TEST" = true ]; then
   echo "****************************************** TEST MODE ************************************************" $>> ${LOG_FILE}
   echo "*****************************************************************************************************" $>> ${LOG_FILE}
fi

echo "Start - Data Elaborazione: $anno-$mese-$giorno - $dd" $>> ${LOG_FILE}

echo "Pulizia file log " $>>${LOG_FILE}
rm /tmp/decompressione.log
touch /tmp/decompressione.log
truncate -s 0 /tmp/decompressione.log

echo "Avvio procedura di decompressione " $>> ${LOG_FILE}
cd /home/silvia/last_release/bin

echo "Avvio procedura di decompressione " $>> ${LOG_FILE}
if [ "$TEST" = true ]; then
  echo "sudo ./flusso-misure.sh -DGAS ${FLUSSO}-T --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}"  &>> ${LOG_FILE}
  echo "sudo ./flusso-misure.sh -DGAS ${FLUSSO}-T --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}"
  sudo ./flusso-misure.sh -DGAS ${FLUSSO}-T --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} &>> /tmp/decompressione.log
else
  sudo ./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} &>> /tmp/decompressione.log
fi
timestamp_decompressione=$(cat /tmp/decompressione.log | grep  "*** timestamp di decompressione:" | awk '{print $5" "$6}')

echo "timestamp_decompressione: ${timestamp_decompressione}" >>  ${LOG_FILE}
echo "timestamp_decompressione: ${timestamp_decompressione}"
cat /tmp/decompressione.log >> ${LOG_FILE}
echo "Decompressione completata " $>>{LOG_FILE}
cd - > /dev/null

if [ "$TEST" = true ]; then
  QUERY_FILTER_DATA="select filename_folder_dest  FROM cmg_gas.report_decompressione_test where codice = '000' and nvl(filename_folder_dest, '') != '' and dataelaborazione = '${timestamp_decompressione}'  and filename_folder_dest like '%${FLUSSO}%'"
else
  QUERY_FILTER_DATA="select filename_folder_dest  FROM cmg_gas.report_decompressione where codice = '000' and nvl(filename_folder_dest, '') != '' and dataelaborazione = '${timestamp_decompressione}'  and filename_folder_dest like '%${FLUSSO}%'"
fi
echo "Query usata per filtro dati:${QUERY_FILTER_DATA}" $>> ${LOG_FILE}
echo "${QUERY_FILTER_DATA}" > /tmp/query.sql

echo "Start Query" $>>  ${LOG_FILE}
if [ "$TEST" = true ]; then
  echo "Disattivato inserimento nella tabella elenco_file"
else
  cat /home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql $>> ${LOG_FILE}
  echo "hive -hiveconf flusso=${FLUSSO} -f /home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql" $>> ${LOG_FILE}
  echo "hive -hiveconf flusso=${FLUSSO} -f /home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql"
  echo "**********   FLUSSO: ${FLUSSO} dataelaborazione: ${timestamp_decompressione} **************" >> ${LOG_FILE}
  hive -hiveconf flusso=${FLUSSO} -hiveconf dataelaborazione="${timestamp_decompressione}" -f "/home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql"  &>> ${LOG_FILE}
  echo "Query inserimento completata" >> ${LOG_FILE}
fi

echo "${CURTMS} flusso $FLUSSO --anno $ANNO --mese $MESE --giorno $GIORNO" &>> ${LOG_FILE}

# Versione che preleva la lista dal query
if [ "$TEST" = true ]; then
  echo "${BASEPATH}/script/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} -t -v"
  echo "${BASEPATH}/script/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} -t -v" >>  ${LOG_FILE}
  ${BASEPATH}/script/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} -t -v &>> ${LOG_FILE}
else
  echo "${BASEPATH}/script/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} -v"
  echo "${BASEPATH}/script/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} -v" >>  ${LOG_FILE}
  ${BASEPATH}/script/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} -v &>> ${LOG_FILE}
fi

DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
echo "END - Elaborazione completata il $dd" $>> ${LOG_FILE}
cd -

echo "sudo rm -rf $PATH_WORK/TMG_*" &>> ${LOG_FILE}
#sudo rm -rf ${PATH_WORK}/TMG_*  &>> ${LOG_FILE}
#sudo rm -rf ${PATH_WORK2}/TMG_*  &>> ${LOG_FILE}


