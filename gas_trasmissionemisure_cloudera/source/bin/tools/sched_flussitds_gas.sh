#!/bin/bash

BASEPATH_ROOT="/mnt/isilonshare1/Software/GAS/"
BASEPATH="${BASEPATH_ROOT}bin"
BASEPATH_CONF="${BASEPATH_ROOT}conf"
BASEPATH_LOG="${BASEPATH_ROOT}log"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
#ANNO=`expr $ANNOS + 0`
#MESE=`date -d "$DD" +%m`
ANNO=$1
MESE=$2
CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE="$BASEPATH_LOG""/log_TDS_GAS__$CURTMS"".txt"

SOURCE="/mnt/Settlement/TSG/TSG1"

#FILE=/home/acutest/20190826_GAS/bin/input.in
FILE=${BASEPATH_ROOT}/bin/input.in
#FILE_LOG=$(date  +"%y%m%d")
DATE=`date "+%Y%m%d"`

echo "Log file: "${LOG_FILE}
rm ${FILE} &> /dev/null

echo "Elabor. dir: $SOURCE/*/$ANNO/$MESE" >> ${LOG_FILE}
for f in `ls -d $SOURCE/*/$ANNO/$MESE`
do
        if [  "$(ls -A $f)" ];
        then
                echo "$f" >> ${FILE}
                #echo "File Elab. `ls $f`"
        fi
done

if test -f "$FILE"; then
    echo "Avvio Elaborazione " >> ${LOG_FILE}
    NUM_EXEC=45
    NUM_EXEC_CORE=5
    NUM_EXEC_MEM=24g
    DRIVER_CORES=5
    DRIVER_MEMORY=24g
    FILES=${BASEPATH_CONF}/log4j.properties#log4j.properties
    CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:../conf/log4j.properties"

    cd ${BASEPATH_ROOT}/pyspark/
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
    ${BASEPATH_ROOT}/pyspark/REQs/recupero_mainJobTDS.py \
    -i ${PATH_GAS}/bin/ -f TDS >> ${LOG_FILE}


else
        echo "File non presente $FILE" >> ${LOG_FILE}
        echo "FILE non presente $FILE"
fi

echo "Fine Elaborazione" >> ${LOG_FILE}
