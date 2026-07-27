#!/bin/bash
#chmod 755

NUM_EXEC=15
NUM_EXEC_CORE=5
NUM_EXEC_MEM=24g
DRIVER_CORES=5
DRIVER_MEMORY=35g
FILES=/home/acutest/GAS/conf/log4j.properties#log4j.properties
CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:../conf/log4j.properties"

reset 

cd /home/acutest/GAS/pyspark/
find . -name "*.pyc" | xargs rm
zip -r --exclude=*LOG* --exclude=*pyc* --exclude=*.log ../bin/gas.zip .

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
--py-files /home/acutest/GAS/bin/gas.zip \
/home/acutest/GAS/pyspark/REQs/mainJobSAG.py \
$* -f SAG



cd -

