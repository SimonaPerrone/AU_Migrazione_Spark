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
truncate -s 0 $FILE_PARTITIONS_CSV

echo "hive -e 'show partitions ${database}.${table}'"

hive -e "show partitions ${database}.${table}" 2> /dev/null 1> $FILE_PARTITIONS_TMP
cat $FILE_PARTITIONS_TMP | grep -v "^WARN" > $FILE_PARTITIONS_DAT


#cat $FILE_PARTITIONS_DAT
CURTMS=`date +%Y%m%d`
echo ${CURTMS}

#echo "hadoop fs -cp $path/$database/$table $path/$database/${CURTMS}_$table"
#hadoop fs -cp "$path/$database/$table" "$path/$database/${CURTMS}_$table"

echo hadoop fs -mkdir "$path/$database/$table_compact"

hadoop fs -mkdir "$path/$database/$table_compact"

bytes=0
# Per ogni partizione
while read line; do
    bytes=$(hadoop fs -du -s ${path}/${database}/${table}/${line}  | awk '{print $1}')

    echo "${path}/${database}/${table}/${line} ($numfile - ${bytes} B)"
    echo "${bytes},${line}" >> $FILE_PARTITIONS_CSV
done <"$FILE_PARTITIONS_DAT"
echo "Fine calcolo file tmp"

echo "Lista partizioni : $(wc -l $FILE_PARTITIONS_DAT)"
#exit
NAME_APP=compacting_v2.py
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
$NAME_APP ${database} ${table} ${path} $FILE_PARTITIONS_CSV

if [ ${flusso} == "" ]; then
  echo "flusso non presente"
  echo "/home/acutest/GAS/bin/tools/compaction/aggiorna_report_compacting.sh FLUSSO"
else
  /home/acutest/GAS/bin/tools/compaction/aggiorna_report_compacting.sh ${flusso}
fi

#echo "hadoop fs -rm -R -skipTrash $path/$database/$table" 
echo "hadoop fs -mv $path/$database/$table $path/$database/${CURTMS}_$table"
echo "hadoop fs -mv $path/$database/$table_compact $path/$database/$table"



