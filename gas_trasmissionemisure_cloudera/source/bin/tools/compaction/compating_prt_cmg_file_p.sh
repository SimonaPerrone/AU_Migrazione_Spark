#/bin/bash

database="cmg_gas"
table="prt_cmg_file_backeted_p_compact"
path="/user/silvia/au/misure_gas_au/cmg_gas"
year=$1
month=$2
flusso=$3

hadoop fs -mkdir /user/silvia/au/misure_gas_au/cmg_gas/compating_tmp

echo "Pulizia path tmp"
hadoop fs -rm -skipTrash /user/silvia/au/misure_gas_au/cmg_gas/compating_tmp/*

path_hdfs_src="$path/$table/t_anno_caricamento=$year/t_mese_caricamento=$month/t_tipo_servizio=$flusso"
# Per ogni partizione
echo "hadoop fs -du -s ${path_hdfs_src} | awk '{print $1}'"
bytes=$(hadoop fs -du -s ${path_hdfs_src} | awk '{print $1}')
echo "Bytes: $bytes"

#exit
NAME_APP=compating_prt_cmg_file_p.py
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
$NAME_APP ${database} ${table} ${path} $bytes $year $month $flusso
#./compating_prt_cmg_file_p.sh cmg_gas prt_cmg_tml_p_compact /user/silvia/au/misure_gas_au/cmg_gas $SIZE 2020 03 TGL



#esempio: ./compating_prt_cmg_file_p.sh 2020 03 TGL
