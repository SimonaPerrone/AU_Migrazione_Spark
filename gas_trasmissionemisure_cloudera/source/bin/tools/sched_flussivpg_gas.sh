#!/bin/bash

BASEPATH_ROOT="/mnt/isilonshare1/Software/GAS"
BASEPATH="${BASEPATH_ROOT}/bin"
BASEPATH_CONF="${BASEPATH_ROOT}/conf"
BASEPATH_LOG="${BASEPATH_ROOT}/log"
BASEPATH_SRC="${BASEPATH_ROOT}/pyspark"

CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
#ANNO=`expr $ANNOS + 0`
#MESE=`date -d "$DD" +%m`
CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE="$BASEPATH_LOG""/log_VPG_GAS__$CURTMS"".txt"

echo
echo "*****************************************************************************************************************************"
echo "File LOG: ${LOG_FILE}"

cd "$BASEPATH" > /dev/null

SOURCE="/mnt/Settlement/TSG/TSG2"
ANNO=$1
MESE=$2

FILE=${BASEPATH_ROOT}/bin/input.in

echo "ANNO: ${ANNO}" >> ${LOG_FILE}
echo "MESE: ${MESE}" >> ${LOG_FILE}
echo "FILE: ${FILE}" >> ${LOG_FILE}

rm ${FILE} &> /dev/null

for f in `ls -d $SOURCE/*/$ANNO/$MESE`
do
        if [  "$(ls -A $f)" ];
        then
            echo "$f" >> ${FILE}
        fi
done


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
	${BASEPATH_SRC}/REQs/mainJobVPG.py \
	-i ${BASEPATH} \
	-f VPG \
	$* >> ${LOG_FILE}

else
        echo "File non presente $FILE" >> ${LOG_FILE}
        echo "FILE non presente $FILE"
fi

echo
echo " Elaborazione Completa"
echo "*****************************************************************************************************************************"

