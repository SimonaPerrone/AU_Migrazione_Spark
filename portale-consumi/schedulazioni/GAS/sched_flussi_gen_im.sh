#!/bin/bash


BASEPATH="/home/acutest/GAS/bin"
BASEPATH_LOG="/home/silvia"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
ANNO=`expr $ANNOS + 0`
MESE=`date -d "$DD" +%m`
GIORNO=`date -d "$DD" +%d`
CURTMS=`date +%Y%m%d%H%M%S`
FLUSSO="$1"
LOG_FILE="$BASEPATH_LOG""/schedulazioni/log_crontab/log_${FLUSSO}_GAS__$CURTMS"".txt"
PATH_WORK="/mnt/isilonshare1/GAS_INJ/isilonshare_gas"
PATH_WORK2="/mnt/isilonshare1/GAS_INJ"


dd=`date`
echo $>> ${LOG_FILE}
echo "*****************************************************************************************************" $>> ${LOG_FILE}
echo "Start - Data Elaborazione: $anno-$mese-$giorno - $dd" $>> ${LOG_FILE}

truncate -s 0 /tmp/decompressione.log
#echo ${CURTMS}" sudo ${BASEPATH}/tools/copia_unzip_v2_master.sh ${MESE} ${GIORNO} ${ANNO} ${FLUSSO}" &>> ${LOG_FILE}
#echo ${CURTMS}" sudo ${BASEPATH}/tools/copia_unzip_v2_master.sh ${MESE} ${GIORNO} ${ANNO} ${FLUSSO}"
#sudo ${BASEPATH}/tools/copia_unzip_v2_master.sh ${MESE} ${GIORNO} ${ANNO} ${FLUSSO}
#sudo ${BASEPATH}/tools/copia_unzip_v2_master.sh ${MESE} ${GIORNO} ${ANNO} ${FLUSSO} &>> ${LOG_FILE}
cd /home/silvia/last_release/bin
echo "sudo ./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}"  &>> ${LOG_FILE}
echo "sudo ./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}"
#sudo ./flusso-misure.sh -DGAS ${FLUSSO} -V --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} ${LOG_FILE}  &>> ${LOG_FILE}
#sudo ./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} &>> ${LOG_FILE}
sudo ./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} &>> /tmp/decompressione.log
timestamp_decompressione=$(cat /tmp/decompressione.log | grep  "*** timestamp di decompressione:" | awk '{print $5" "$6}')

echo "timestamp_decompressione: ${timestamp_decompressione}" >>  ${LOG_FILE}
echo "timestamp_decompressione: ${timestamp_decompressione}"
cat /tmp/decompressione.log >> ${LOG_FILE}

cd - > /dev/null
echo "select filename_folder_dest  FROM cmg_gas.report_decompressione where codice = '000' and nvl(filename_folder_dest, '') != '' and dataelaborazione = '${timestamp_decompressione}'  and filename_folder_dest like '%${FLUSSO}%'" > /tmp/query.sql
#DEBUG END
#exit

#${BASEPATH}/ingestion_data_im1.sh -i  ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO} &>> ${LOG_FILE}
${BASEPATH}/ingestion_data_im1.sh -i  ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO} &>> ${LOG_FILE}

DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
echo "END - Elaborazione completata il $dd" &>>  ${LOG_FILE}

echo "sudo rm -rf ${PATH_WORK2}/TMG_*" &>>  ${LOG_FILE}
sudo rm -rf ${PATH_WORK2}/TMG_*
