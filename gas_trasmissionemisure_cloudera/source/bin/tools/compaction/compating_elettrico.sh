#/bin/bash

database=$1 #cmg_gas
table=$2 #prt_cmg_sw1_test_c
table_compact=$2"_compact"
path=$3 #/user/silvia/au/misure_gas_au
flusso=$4


FILE_PARTITIONS_DAT=/tmp/${database}_${table}_partitions.dat
touch $FILE_PARTITIONS_DAT
truncate -s 0 $FILE_PARTITIONS_DAT

FILE_PARTITIONS_TMP=/tmp/${database}_${table}_partitions_tmp.dat
FILE_PARTITIONS_CSV=/tmp/${database}_${table}_partitions.csv

touch $FILE_PARTITIONS_CSV
touch $FILE_PARTITIONS_TMP

truncate -s 0 $FILE_PARTITIONS_TMP

echo "hive -e 'show partitions ${database}.${table}'"

hive -e "show partitions ${database}.${table}" 2> /dev/null 1> $FILE_PARTITIONS_TMP
cat $FILE_PARTITIONS_TMP | grep -v "^WARN" > $FILE_PARTITIONS_DAT

echo "Lista partizioni : $(wc -l $FILE_PARTITIONS_DAT)"

NAME_APP=compating_elettrico.py
PATH_APP=/home/acutest/GAS/
PATH_CONF=/home/acutest/GAS/conf/log4j.properties
FILES=$PATH_APP/conf/log4j.properties#log4j.properties
CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:$PATH_CONF"
NUM_EXEC=10
NUM_EXEC_CORE=10
NUM_EXEC_MEM=50g
DRIVER_CORES=10
DRIVER_MEMORY=40g
spark-submit \
--conf $CONF \
--files $FILES \
--num-executors $NUM_EXEC \
--executor-cores $NUM_EXEC_CORE \
--executor-memory $NUM_EXEC_MEM \
--driver-cores $DRIVER_CORES \
--driver-memory $DRIVER_MEMORY \
$NAME_APP ${database} ${table} ${path} $FILE_PARTITIONS_DAT


