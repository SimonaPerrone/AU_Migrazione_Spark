#!/bin/bash
#chmod 755
#

#export HADOOP_USER_NAME=ec2-user


PATH_APP=/home/acutest/GAS/

NUM_EXEC=15
NUM_EXEC_CORE=15
NUM_EXEC_MEM=60g
DRIVER_CORES=5
DRIVER_MEMORY=40g
FILES=$PATH_APP/conf/log4j.properties#log4j.properties
CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:../conf/log4j.properties"

PATH_SRC=$PATH_APP/pyspark/
ZIP_FILE=$PATH_APP/bin/gas.zip
MAIN_FILE=$PATH_SRC/Main.py

#clear
echo
echo "------------------------ START ------------------------ "
echo "Elaborazione eseguita il `date`"
cd $PATH_SRC

find . -name "*.pyc" | xargs rm 2>/dev/null
find . -name "*.log" | xargs rm 2>/dev/null
zip -r --exclude=*LOG* --exclude=*pyc* --exclude=*log $ZIP_FILE . 1> /dev/null

spark-submit \
--num-executors $NUM_EXEC \
--executor-cores $NUM_EXEC_CORE \
--executor-memory $NUM_EXEC_MEM \
--driver-cores $DRIVER_CORES \
--driver-memory $DRIVER_MEMORY \
--conf $CONF \
--conf spark.yarn.executor.memoryOverhead=5120 \
--files $FILES \
--py-files $ZIP_FILE \
$MAIN_FILE \
"$@"

result=$?

cd -

echo "Risultato elaborazione $result"
echo $result 1>&2
echo "Elaborazione completata il `date`"
echo "------------------------ END ------------------------ "

