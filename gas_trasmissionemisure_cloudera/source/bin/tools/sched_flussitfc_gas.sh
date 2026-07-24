#!/bin/bash

BASEPATH_ROOT="/mnt/isilonshare1/Software/GAS/"
BASEPATH="${BASEPATH_ROOT}bin"
BASEPATH_LOG="${BASEPATH_ROOT}log"
BASEPATH_SRC="${BASEPATH_ROOT}pyspark"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
ANNO=`expr $ANNOS + 0`
MESE=`date -d "$DD" +%m`
CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE="$BASEPATH_LOG""/log_TFC_GAS__$CURTMS"".txt"

echo
echo "*****************************************************************************************************************************" 
echo "File LOG: ${LOG_FILE}"

SOURCE=/mnt/Settlement/TSG/TSG2

ANNO=$1
MESE=$2
FILE=${BASEPATH}/input.in

echo "SOURCE: ${SOURCE}" >> ${LOG_FILE}
echo "ANNO: ${ANNO}" >> ${LOG_FILE}
echo "MESE: ${MESE}" >> ${LOG_FILE}
echo "FILE: ${FILE}" >> ${LOG_FILE}
echo "Entry point: "${BASEPATH_SRC}/REQs/mainJobTFC.py >> ${LOG_FILE}

rm ${FILE} &> /dev/null
err=$(ls -d  $SOURCE/*/$ANNO/$MESE 2>&1 > /dev/null)
if [ -z "$err" ]
then
      echo
else
      echo "ERR: ${err}"
      exit
fi

for f in `ls -d $SOURCE/*/$ANNO/$MESE`
do
        if [  "$(ls -A $f)" ];
        then
                echo "$f" >> ${LOG_FILE}
                echo "$f" >> ${FILE}
        fi
done

test -f $FILE
if test -f "$FILE"; then
        echo "Start" >> ${LOG_FILE} 

	NUM_EXEC=45
	NUM_EXEC_CORE=5
	NUM_EXEC_MEM=24g
	DRIVER_CORES=5
	DRIVER_MEMORY=24g
	FILES=${BASEPATH_ROOT}/conf/log4j.properties#log4j.properties
	CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:../conf/log4j.properties"



	cd ${BASEPATH_SRC}
	find . -name "*.pyc" | xargs rm 2> /dev/null
	zip -r --exclude=*LOG* --exclude=*pyc* --exclude=*.log ../bin/gas.zip . 1> /dev/null

	spark-submit \
		--num-executors $NUM_EXEC \
		--executor-cores $NUM_EXEC_CORE \
		--executor-memory $NUM_EXEC_MEM \
		--driver-cores $DRIVER_CORES \
		--driver-memory $DRIVER_MEMORY \
		--conf $CONF \
		--files $FILES \
		--conf $CONF \
		--files $FILES \
		--py-files ${BASEPATH_ROOT}/bin/gas.zip \
		${BASEPATH_SRC}/REQs/mainJobTFC.py \
		-i ${BASEPATH} -f TFC \
		$* >> ${LOG_FILE}

	cd -

else
        echo "File non presente $FILE" >> ${LOG_FILE}
        echo "FILE non presente $FILE"
fi

echo " Elaborazione Completa"
echo "*****************************************************************************************************************************"
