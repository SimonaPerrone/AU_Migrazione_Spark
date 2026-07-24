PATH_ROOT="/mnt/isilonshare1/Software/GAS"
BASEPATH_LOG="${PATH_ROOT}log"

CURTMS=`date +%Y%m%d%H%M%S`

LOG_FILE="$BASEPATH_LOG""/log_VERIFICHE_TXT_GAS__$CURTMS"".txt"
echo "Inizio ricerca e verifiche sui file TXT: ${CURDD}" >> ${LOG_FILE}
cd ${PATH_ROOT}/bin/tools/reports/ >> ${LOG_FILE}

DD=`date +%Y-%m-%d -d "$1-$2-$3"`

#for i in {1..31}
i=$3
while [ $i -gt 1 ]
do


	ANNO=`date -d "$DD" +%Y`
	MESE=`date -d "$DD" +%m`
	GIORNO=`date -d "$DD" +%d`

	echo "${DD}  ${GIORNO}"


	echo "sudo ./recupero_txt.sh ${ANNO} ${MESE} ${GIORNO} >> ${LOG_FILE}"
	sudo ./recupero_txt.sh ${ANNO} ${MESE} ${GIORNO} >> ${LOG_FILE}

	DD=`date +%Y-%m-%d -d "$ANNO-$MESE-$GIORNO -1 day"`
        i=`date -d "$DD" +%d`

done

ANNO=`date -d "$DD" +%Y`
MESE=`date -d "$DD" +%m`
GIORNO=`date -d "$DD" +%d`
echo "sudo ./recupero_txt.sh ${ANNO} ${MESE} ${GIORNO} >> ${LOG_FILE}"
sudo ./recupero_txt.sh ${ANNO} ${MESE} ${GIORNO} >> ${LOG_FILE}

echo "Fine ricerca e verifiche sui file TXT: ${CURDD}" >> ${LOG_FILE}
