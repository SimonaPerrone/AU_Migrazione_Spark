#/bin/bash

database=$1 #cmg_gas
table=$2 #prt_cmg_sw1_test_c
table_compact=$2"_compact"
path=$3 #/user/silvia/au/misure_gas_au

echo hadoop fs -mkdir "$path/$database/$table_compact"
hadoop fs -mkdir "$path/$database/$table_compact"

FILE_PARTITIONS_CSV=/tmp/${database}_${table}_partitions.csv
bytes=$(hadoop fs -du -s ${path}/${database}/${table} | awk '{print $1}')
numfile=$(hadoop fs -du -s ${path}/${database}/${table} | wc -l)

line=${table}
echo "${bytes}," > $FILE_PARTITIONS_CSV
cat $FILE_PARTITIONS_CSV

NAME_APP=compating_v2.py
PATH_APP=/home/acutest/GAS/
PATH_CONF=/home/acutest/GAS/conf/log4j.properties
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
$NAME_APP ${database} ${table} ${path} $FILE_PARTITIONS_CSV


