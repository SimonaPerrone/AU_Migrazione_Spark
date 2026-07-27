#!/bin/bash
ENABLE_CHECK_FILE=true
NO_REDIRECT_TO_ING=false

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

rm /tmp/decompressione.log
touch /tmp/decompressione.log
#truncate -s /tmp/decompressione.log
#echo ${CURTMS}" sudo ${BASEPATH}/tools/copia_unzip_v2_master.sh ${MESE} ${GIORNO} ${ANNO} ${FLUSSO}" &>> ${LOG_FILE}
#echo ${CURTMS}" sudo ${BASEPATH}/tools/copia_unzip_v2_master.sh ${MESE} ${GIORNO} ${ANNO} ${FLUSSO}" 
#sudo ${BASEPATH}/tools/copia_unzip_v2_master.sh ${MESE} ${GIORNO} ${ANNO} ${FLUSSO}
#sudo ${BASEPATH}/tools/copia_unzip_v2_master.sh ${MESE} ${GIORNO} ${ANNO} ${FLUSSO} &>> ${LOG_FILE}
cd /home/silvia/last_release/bin
echo "sudo ./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}"  &>> ${LOG_FILE}
echo "sudo ./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO}"
#sudo ./flusso-misure.sh -DGAS ${FLUSSO} -V --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} ${LOG_FILE}  &>> ${LOG_FILE}
sudo ./flusso-misure.sh -DGAS ${FLUSSO} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} &>> /tmp/decompressione.log
timestamp_decompressione=$(cat /tmp/decompressione.log | grep  "*** timestamp di decompressione:" | awk '{print $5" "$6}')

cat /tmp/decompressione.log >> ${LOG_FILE}
echo "timestamp_decompressione: ${timestamp_decompressione}"
echo "timestamp_decompressione: ${timestamp_decompressione}" >> ${LOG_FILE}
cd - > /dev/null

if ${NO_REDIRECT_TO_ING}
then
	echo "sudo rsync -a  $PATH_WORK/TMG_* ${PATH_WORK2}"  &>> ${LOG_FILE}
	#sudo mv $PATH_WORK/TMG_* /mnt/isilonshare1/GAS_INJ 
	sudo rsync -a  $PATH_WORK/TMG_* ${PATH_WORK2} 
	#${BASEPATH}/tools/copia_unzip_v2_master.sh ${MESE} ${GIORNO} ${ANNO} ${FLUSSO} &>> ${LOG_FILE}

	#exit

	echo "Start Query" $>>  ${LOG_FILE}
	cat /home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql $>> ${LOG_FILE}
	echo "hive -hiveconf flusso=${FLUSSO} -f /home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql" $>> ${LOG_FILE}
	echo "hive -hiveconf flusso=${FLUSSO} -f /home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql" 
	hive -hiveconf flusso=${FLUSSO} -hiveconf dataelaborazione="${timestamp_decompressione}" -f "/home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql"  &>> ${LOG_FILE}
	#DEBUG START
	#truncate -s 0 /tmp/result.csv
	#truncate -s 0 /tmp/result.err

	echo "find /mnt/isilonshare1/GAS_INJ/TMG_*/DISTRIBUTORE/* -type f > /tmp/res.dat" &>> ${LOG_FILE}
	find /mnt/isilonshare1/GAS_INJ/TMG_*/DISTRIBUTORE/* -type f > /tmp/res.dat
	echo "sudo python /home/acutest/GAS/bin/tools/check_file.py /tmp/res.dat $FLUSSO"
	echo "sudo python /home/acutest/GAS/bin/tools/check_file.py /tmp/res.dat $FLUSSO" &>> ${LOG_FILE}
	sudo python /home/acutest/GAS/bin/tools/check_file.py /tmp/res.dat $FLUSSO &>> ${LOG_FILE}
	#sudo python /home/acutest/GAS/bin/tools/check_files2.py /tmp/res.dat $FLUSSO
	sudo mv /tmp/result.csv /tmp/${FLUSSO}${CURTMS}_result.csv
	sudo mv /tmp/result.err /tmp/${FLUSSO}${CURTMS}_result.err

	sudo chown silvia:silvia /tmp/${FLUSSO}${CURTMS}_result.csv
	sudo chown silvia:silvia /tmp/${FLUSSO}${CURTMS}_result.err

	echo "hadoop fs -put /tmp/${FLUSSO}${CURTMS}_result.csv /user/silvia/au/misure_gas_au/cmg_gas/listfile_debug" &>>  ${LOG_FILE}
	echo "hadoop fs -put /tmp/${FLUSSO}${CURTMS}_result.csv /user/silvia/au/misure_gas_au/cmg_gas/listfile_debug"
	hadoop fs -put /tmp/${FLUSSO}${CURTMS}_result.csv /user/silvia/au/misure_gas_au/cmg_gas/listfile_debug
	echo "hadoop fs -put /tmp/${FLUSSO}${CURTMS}_result.err /user/silvia/au/misure_gas_au/cmg_gas/listfile_debug" &>> ${LOG_FILE}
	echo "hadoop fs -put /tmp/${FLUSSO}${CURTMS}_result.err /user/silvia/au/misure_gas_au/cmg_gas/listfile_debug"
	hadoop fs -put /tmp/${FLUSSO}${CURTMS}_result.err /user/silvia/au/misure_gas_au/cmg_gas/listfile_debug


	sudo rm /tmp/${FLUSSO}${CURTMS}_result.csv
	sudo rm /tmp/${FLUSSO}${CURTMS}_result.err

	##find /mnt/isilonshare1/GAS_INJ/ -type f | grep '[a-zA-Z\/\_0-9]*\/\.[a-zA-Z\/\_0-9]*.xml'
	if ${ENABLE_CHECK_FILE}
	then
		##Mostra la lista dei file scartati per via del punto prima del nome del file
		for FILE_NAME in $(find /mnt/isilonshare1/GAS_INJ/ -type f | grep '[a-zA-Z\/\_0-9]*\/\.[a-zA-Z\/\_0-9]*.xml')
		do 
		    #echo $(basename ${FILE_NAME}) |  awk '{print substr($0, 2, length($0))}'
		    echo "ATTENZIONE: FILE SCARTATI ${FILE_NAME}" >> ${LOG_FILE}
		done
	fi
else

#DEBUG END
#exit
	#echo "select regexp_replace(filename_folder_dest, '/isilonshare_gas/', '/') FROM cmg_gas.report_decompressione where nvl(filename_folder_dest, '') != '' and cast(dataelaborazione as date) = CURRENT_DATE and filename_folder_dest like '%${FLUSSO}%'" > /tmp/query.sql
	echo "select filename_folder_dest  FROM cmg_gas.report_decompressione where codice = '000' and nvl(filename_folder_dest, '') != '' and dataelaborazione = '${timestamp_decompressione}'  and filename_folder_dest like '%${FLUSSO}%'" > /tmp/query.sql
fi


echo "Start Query" $>>  ${LOG_FILE}
cat /home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql $>> ${LOG_FILE}
echo "hive -hiveconf flusso=${FLUSSO} -f /home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql" $>> ${LOG_FILE}
echo "hive -hiveconf flusso=${FLUSSO} -f /home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql" 
hive -hiveconf flusso=${FLUSSO} -hiveconf dataelaborazione="${timestamp_decompressione}" -f "/home/acutest/GAS/bin/tools/sql/query_insert_elenco_sched.sql"  &>> ${LOG_FILE}


echo ${CURTMS}" flusso $FLUSSO --anno $ANNO --mese $MESE --giorno $GIORNO" &>> ${LOG_FILE}
#echo "${BASEPATH}/ingestion_data_debug.sh -i ${PATH_WORK2} --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} -f ${FLUSSO}"

# Versione che preleva la lista dal query
#echo "${BASEPATH}/ingestion_data_debug.sh -i ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO}"
#${BASEPATH}/ingestion_data_debug.sh -i  ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO} &>> ${LOG_FILE}

if ${NO_REDIRECT_TO_ING}
then
	#VERSIONE che cerca i file nella directory
	${BASEPATH}/ingestion_data_debug.sh -i /mnt/isilonshare1/GAS_INJ --anno ${ANNO} --mese ${MESE} --giorno ${GIORNO} -f ${FLUSSO} &>> ${LOG_FILE}
	${BASEPATH}/ingestion_data_debug.sh -i  ${PATH_WORK2} --anno ${ANNO}  --mese ${MESE} --giorno ${GIORNO} -f ${FLUSSO} &>> ${LOG_FILE}
else
	# Versione che preleva la lista dal query
	echo "${BASEPATH}/ingestion_data_debug.sh -i ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO}"
	echo "${BASEPATH}/ingestion_data_debug.sh -i  ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO}" >> ${LOG_FILE}
	${BASEPATH}/ingestion_data_debug.sh -i  ${PATH_WORK2} -e /tmp/query.sql -f ${FLUSSO} &>> ${LOG_FILE}
fi

DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
echo "END - Elaborazione completata il $dd" $>> ${LOG_FILE}
cd -

#echo "sudo rm -rf $PATH_WORK/TMG_*" &>> ${LOG_FILE}
#sudo rm -rf ${PATH_WORK}/TMG_*  &>> ${LOG_FILE}
#sudo rm -rf ${PATH_WORK2}/TMG_*  &>> ${LOG_FILE}


