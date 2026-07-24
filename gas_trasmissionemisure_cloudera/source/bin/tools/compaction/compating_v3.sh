#/bin/bash

database=$1 #cmg_gas
table=$2 #prt_cmg_sw1_test_c
path=$3 #/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_sw1_test_c
partitions=$4 #annomese=202001

#echo "hadoop fs -mkdir ${path}/${table}_backup" 
#echo "hadoop fs -mkdir ${path}/${table}_backup/$partitions"


# Per ogni partizione
echo "hadoop fs -du -s ${path}/${table}/$partitions | awk '{print $1}'"
bytes=$(hadoop fs -du -s ${path}/${table}/$partitions | awk '{print $1}')
echo "Bytes: $bytes"

#exit
NAME_APP=compating_v3.py
PATH_APP=/mnt/isilonshare1/Software/GAS/
PATH_CONF=/mnt/isilonshare1/Software/GAS/conf/log4j.properties
FILES=$PATH_APP/conf/log4j.properties#log4j.properties
CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:$PATH_CONF"
NUM_EXEC=10
NUM_EXEC_CORE=10
NUM_EXEC_MEM=50g
DRIVER_CORES=5
DRIVER_MEMORY=40g
spark-submit \
--conf $CONF \
--files $FILES \
--num-executors $NUM_EXEC \
--executor-cores $NUM_EXEC_CORE \
--executor-memory $NUM_EXEC_MEM \
--driver-cores $DRIVER_CORES \
--driver-memory $DRIVER_MEMORY \
$NAME_APP ${database} ${table} ${path} $bytes $partitions


