!/bin/bash
ENABLE_CHECK_FILE=true
NO_REDIRECT_TO_ING=false

BASEPATH="/mnt/isilonshare1/Software/GAS/pyspark2"
BASEPATH_LOG="/mnt/isilonshare1/Software/GAS/log"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
CURTMS=`date +%Y%m%d%H%M%S`
FLUSSO="$1"
LOG_FILE="$BASEPATH_LOG""/log_${FLUSSO}_GAS__$CURTMS"".txt"
PATH_WORK="/mnt/isilonshare1/GAS_INJ/isilonshare_gas"
PATH_WORK2="/mnt/isilonshare1/GAS_INJ"
PATH_DECOMRPESSIONE="/mnt/isilonshare1/Software/EE/bin/" 
PATH_TOOLS="/mnt/isilonshare1/Software/GAS/bin/tools"
dd=`date`


#ANNO=`expr $ANNOS + 0`
#MESE=`date -d "$DD" +%m`
#GIORNO=`date -d "$DD" +%d`

ANNO=$2
MESE=$3
GIORNO=$4

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
cd ${PATH_DECOMRPESSIONE}

echo "Avvio procedura di decompressione " $>> ${LOG_FILE}
if [ "$TEST" = true ]; then
  echo "./flusso-misure.sh -DGAS ${FLUSSO}-T --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}"  &>> ${LOG_FILE}
  echo "./flusso-misure.sh -DGAS ${FLUSSO}-T --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}"
  ./flusso-misure.sh -DGAS ${FLUSSO}-T --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} &>> /tmp/decompressione.log
else
  ./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} &>> /tmp/decompressione.log
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
  cat ${PATH_TOOLS}/sql/query_insert_elenco_sched.sql $>> ${LOG_FILE}
  echo "hive -hiveconf flusso=${FLUSSO} -f ${PATH_TOOLS}/sql/query_insert_elenco_sched.sql" $>> ${LOG_FILE}
  echo "hive -hiveconf flusso=${FLUSSO} -f ${PATH_TOOLS}/sql/query_insert_elenco_sched.sql"
  echo "**********   FLUSSO: ${FLUSSO} dataelaborazione: ${timestamp_decompressione} **************" >> ${LOG_FILE}
  hive -hiveconf flusso=${FLUSSO} -hiveconf dataelaborazione="${timestamp_decompressione}" -f "${PATH_TOOLS}/sql/query_insert_elenco_sched.sql"  &>> ${LOG_FILE}
  echo "Query inserimento completata" >> ${LOG_FILE}
fi

echo "${CURTMS} flusso $FLUSSO --anno $ANNO --mese $MESE --giorno $GIORNO" &>> ${LOG_FILE}

# Versione che preleva la lista dal query
if [ "$TEST" = true ]; then
  echo "${PATH_TOOLS}/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} -t -v"
  echo "${PATH_TOOLS}/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} -t -v" >>  ${LOG_FILE}
  ${PATH_TOOLS}/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} -t -v &>> ${LOG_FILE}
else
  echo "${PATH_TOOLS}/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO}"
  echo "${PATH_TOOLS}/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO}" >>  ${LOG_FILE}
  ${PATH_TOOLS}/ingestion_data_flussi2.sh -e /tmp/query.sql -f ${FLUSSO} &>> ${LOG_FILE}
fi

DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
echo "END - Elaborazione completata il $dd" $>> ${LOG_FILE}
cd -

echo "rm -rf $PATH_WORK/TMG_*" &>> ${LOG_FILE}
#rm -rf ${PATH_WORK}/TMG_*  &>> ${LOG_FILE}
#rm -rf ${PATH_WORK2}/TMG_*  &>> ${LOG_FILE}


