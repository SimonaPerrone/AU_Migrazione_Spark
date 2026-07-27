#!/bin/bash

num_ms=$1
is_4mesi=1
if [[ (! -v num_ms) || -z "$num_ms" ]];then
 num_ms=37
 is_4mesi=0
fi


#AVVIO STORICO GAS
/mnt/isilonshare1/Software/EE/portale_consumi/run_storic_pc_gas.sh "$num_ms"  


cd /mnt/isilonshare1/Software/EE/bin/

CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE_STORIC="/mnt/isilonshare1/Software/EE/portale_consumi/logs/log_PC_STORIC_EE_$CURTMS"".txt"

echo "$(date) - ELABORAZIONE STORICO ELETTRICO"  >> "$LOG_FILE_STORIC"
./flusso-misure-pc.sh -PC_MS ST-RSEE -nm "$num_ms" >> "$LOG_FILE_STORIC"

if [[ "$is_4mesi" == "1" ]];then
 exit
fi

error=$(grep -rli 'org.apache.spark.SparkException' "$LOG_FILE_STORIC")
error="$error"$(grep -rli 'Exception' "$LOG_FILE_STORIC")
error="$error"$(grep -rli 'insufficient memory' "$LOG_FILE_STORIC")

if [ -n "$error" ]
then
 echo "Elaborazione STORICO ELETTRICO non andata a buon fine" >> "$LOG_FILE_STORIC"
 exit 1
fi

hdfs dfs -rm -R -f /user/hive/warehouse/acquirente_unico/misure.db/misure_storic_f2_old >> "$LOG_FILE_STORIC"
hdfs dfs -mv /user/hive/warehouse/acquirente_unico/misure.db/misure_storic_f2 /user/hive/warehouse/acquirente_unico/misure.db/misure_storic_f2_old >> "$LOG_FILE_STORIC"

hive -e "DROP TABLE misure.misure_storic_f2;" >> "$LOG_FILE_STORIC"
hive -e "CREATE TABLE misure.misure_storic_f2(
  cf_piva string,
  pod string,
  data_lettura string,
  data_ricezione string,
  motivazione string,
  lettura_monoraria double,
  lettura_f1 double,
  lettura_f2 double,
  lettura_f3 double,
  lettura_f4 double,
  lettura_f5 double,
  lettura_f6 double,
  ea string,
  er string,
  tipo_flusso string,
  annomese_riferimento int,
  data_lettura_num bigint)
PARTITIONED BY (
  cod_pod char(2),
  is_mis_oraria char(1))
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
TBLPROPERTIES('parquet.compression' = 'SNAPPY');" >> "$LOG_FILE_STORIC"


hdfs dfs -rm -R -f /user/hive/warehouse/acquirente_unico/misure.db/misure_storic_f2 >> "$LOG_FILE_STORIC"
hdfs dfs -mv /user/hive/warehouse/acquirente_unico/misure.db/misure_storiche/misure/misure_storic_f /user/hive/warehouse/acquirente_unico/misure.db/misure_storic_f2 >> "$LOG_FILE_STORIC"

hive -e "MSCK REPAIR TABLE misure.misure_storic_f2;" >> "$LOG_FILE_STORIC"
hive -e "DROP TABLE IF EXISTS storic_tmp;" >> "$LOG_FILE_STORIC"
hive -e "DROP TABLE IF EXISTS misure_storic_f;"; >> "$LOG_FILE_STORIC"

ssh dmphclo14 "impala-shell -q \"use default; INVALIDATE METADATA; use misure; INVALIDATE METADATA; REFRESH misure_storic_f2;\"" >> "$LOG_FILE_STORIC"
 
ssh dmphclo14 "impala-shell -q \"use misure; COMPUTE STATS misure_storic_f2;\"" >> "$LOG_FILE_STORIC" 


hdfs dfs -rm -R -f -skipTrash /user/leonardo/.Trash/Current/*

