#!/bin/bash
num_ms=$1

if [[ (! -v num_ms) || -z "$num_ms" ]];then
 num_ms=37
fi

cd /mnt/isilonshare1/Software/EE/bin/

CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE_STORIC="/mnt/isilonshare1/Software/EE/portale_consumi/logs/log_PC_STORIC_GAS_$CURTMS"".txt"

echo "$(date) - ELABORAZIONE STORICO GAS"  &>> "$LOG_FILE_STORIC"
./flusso-misure-pc2.sh -PC_MS ST-RSGAS -nm "$num_ms" &>> "$LOG_FILE_STORIC" 

error=$(grep -rli 'org.apache.spark.SparkException' "$LOG_FILE_STORIC")
error="$error"$(grep -rli 'Exception' "$LOG_FILE_STORIC")
error="$error"$(grep -rli 'insufficient memory' "$LOG_FILE_STORIC")

if [ -n "$error" ]
then
 echo "Elaborazione STORICO GAS non andata a buon fine" &>> "$LOG_FILE_STORIC"
 exit 1
fi


 echo "AGGIORNAMENTO STRUTTURA DATI STORICO GAS " &>> "$LOG_FILE_STORIC"
 
 hdfs dfs -rm -R -f /user/hive/warehouse/misuregas.db/misure_storic_f2_old &>> "$LOG_FILE_STORIC"
 hdfs dfs -mv /user/hive/warehouse/misuregas.db/misure_storic_f2 /user/hive/warehouse/misuregas.db/misure_storic_f2_old &>> "$LOG_FILE_STORIC"

 hive -e "DROP TABLE misuregas.misure_storic_f2;" &>> "$LOG_FILE_STORIC"
 hive -e "CREATE TABLE misuregas.misure_storic_f2(
 cf_piva string,
 pdr string,
 annomese_riferimento string,
 data_lettura string,
 dt_caricamento string,
 flusso string,
 motivazione string,
 let_tot_prel string)
 PARTITIONED BY (
 cod_pdr char(3)
 )
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 TBLPROPERTIES('parquet.compression' = 'SNAPPY');" &>> "$LOG_FILE_STORIC"
 

 hdfs dfs -rm -R -f /user/hive/warehouse/misuregas.db/misure_storic_f2 &>> "$LOG_FILE_STORIC"
 hdfs dfs -mv /user/hive/warehouse/acquirente_unico/misure.db/misure_storiche/storic_tmp_gas /user/hive/warehouse/misuregas.db/misure_storic_f2 &>> "$LOG_FILE_STORIC"

 hive -e "MSCK REPAIR TABLE misuregas.misure_storic_f2;" &>> "$LOG_FILE_STORIC"
 hive -e "DROP TABLE IF EXISTS storic_tmp_gas"; &>> "$LOG_FILE_STORIC"

 ssh dmphclo14 "impala-shell -q \"use default; INVALIDATE METADATA; use misuregas; INVALIDATE METADATA; REFRESH misure_storic_f2;\"" &>> "$LOG_FILE_STORIC"

 ssh dmphclo14 "impala-shell -q \"use misuregas; COMPUTE STATS misure_storic_f2;\"" &>> "$LOG_FILE_STORIC" & 


