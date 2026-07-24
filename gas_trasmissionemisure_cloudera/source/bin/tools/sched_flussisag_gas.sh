#!/bin/bash
#chmod 755

CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
#ANNO=`expr $ANNOS + 0`
#MESE=`date -d "$DD" +%m`
ANNO=$1
MESE=$2
CURTMS=`date +%Y%m%d%H%M%S`

BASEPATH_ROOT="/mnt/isilonshare1/Software/GAS/"
BASEPATH="${BASEPATH_ROOT}bin"
BASEPATH_LOG="${BASEPATH_ROOT}log"
BASEPATH_SRC="${BASEPATH_ROOT}pyspark"
BASEPATH_CONF="${BASEPATH_ROOT}conf"
LOG_FILE="$BASEPATH_LOG""/log_TDS_GAS__$CURTMS"".txt"

echo
echo "*****************************************************************************************************************************"
echo "File LOG: ${LOG_FILE}"

# TODO Cambiare questo valore
#SOURCE=$3
SOURCE="/mnt/isilonshare1/TISG_SAG1/20190724/"

#INPUT_TISG_DIR="/mnt/isilonshare1/TISG_SAG1/"
#INPUT_TISG_DIR="/mnt/isilonshare1/20191029_TISG_SAG/"

#INPUTDIR="/home/acu/AU/PY/bin/"

FILE=${BASEPATH_ROOT}bin/input.in
#INPUTFILE="/home/acutest/20190826_GAS/bin/input.in"

echo "ANNO: ${ANNO}" >> ${LOG_FILE}
echo "MESE: ${MESE}" >> ${LOG_FILE}
echo "FILE: ${FILE}" >> ${LOG_FILE}

rm $FILE >> ${LOG_FILE}

for f in $(ls -d $SOURCE)
do
        if [  "$(ls -A $f)" ];
        then
                #echo "$f" >> /home/acutest/GAS/bin/input.in
                echo "$f" >> $FILE
        fi
done

if test -f "$FILE"; then
    echo "Start" >> ${LOG_FILE}

    NUM_EXEC=15
    NUM_EXEC_CORE=5
    NUM_EXEC_MEM=24g
    DRIVER_CORES=5
    DRIVER_MEMORY=35g
    FILES=${BASEPATH_CONF}/log4j.properties#log4j.properties
    CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:../conf/log4j.properties"
    LOG_FILE="$BASEPATH_LOG""/log_SAG_GAS__$CURTMS"".txt"

    cd ${BASEPATH_SRC} > /dev/null
    find . -name "*.pyc" | xargs rm
    zip -r --exclude=*LOG* --exclude=*pyc* --exclude=*.log ../bin/gas.zip . >> /dev/null

    echo "Avvio spark-submit"


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
    ${BASEPATH_SRC}/REQs/mainJobSAG.py \
    -i ${BASEPATH} \
    -f SAG \
    -p 1

else
        echo "File non presente $FILE" >> ${LOG_FILE}
        echo "FILE non presente $FILE"
fi
echo " Elaborazione Completa"
echo "*****************************************************************************************************************************"
