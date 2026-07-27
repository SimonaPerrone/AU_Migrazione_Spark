#!/bin/bash

LOG_F=$1
fcheck=$2
fase=$3
max_date_gas=$4


#IMPOSTAZIONE PERIODO MASSIMO PER GAS
./set_periodo/start_period.sh $max_date_gas
limit_gg_gas=$(cat /mnt/isilonshare1/Software/EE/portale_consumi/set_periodo/.periodo)

if [[ (! -v limit_gg_gas) || -z "$limit_gg_gas" ]];then
 limit_gg_gas=1126
 echo "Periodo massimo non impostato. Impostazione a 36 mesi di default" &>> "$LOG_F"
fi

export limit_gg_gas

annomesegiorno_limit=`date -d 'now - '"$limit_gg_gas"' days' '+%Y%m%d'`
echo "anno mese giorno iniziale : $annomesegiorno_limit" &>> "$LOG_F"
echo "data max gas: $max_date_gas" &>> "$LOG_F"
echo "numero di giorni : $limit_gg_gas " &>> "$LOG_F"

rm -f "$fcheck"

if [[ $fase == "1" ]]
then
 echo "$(date) - ELABORAZIONE FORNITURE GAS" &>> "$LOG_F"
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/1_1_Gas_Residenza.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/1_2_Gas_DatiPrelievo.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/1_3_Gas_Misuratore.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/1_4_Gas_Distributori.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/1_5_Gas_Switch.sql  &>> "$LOG_F" &

 tabellex=( \
 RCUGAS_RESIDENZA \
 RCUGAS_PDR_DATIPRELIEVO \
 RCUGAS_PDR_MISURATORE \
 RCUGAS_CONNESSIONI_DISTR \
 v_RCUGAS_DISTRIBUTORE \
 PRT_SWG \
 )
 sleep 3

 ./check_exists_tbl.sh "misuregas" ${tabellex[@]} &>> "$LOG_F"

 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/1_6_Gas_Forniture.sql  &>> "$LOG_F"

fi
 
if [[ $fase == "1" || $fase == "3" ]]
then

 echo "$(date) - PRELABORAZIONE MISURE GAS" &>> "$LOG_F"
 export HIVE_SKIP_SPARK_ASSEMBLY=true; 
 
 tf="TML"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/get_last_elab_gas.sql)
 dtIN=(${dt// / })
 
 if [[ $dtIN == "WARN:" || $dtIN == "" ]];then
  dtIN=19990101
 fi

 echo "$tf : $dtIN" &>> "$LOG_F" 
 hive --hiveconf last_dt_elab_tml="$dtIN" --hiveconf tipof="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/2_1_CreateTableParquetMisureGas_TML.sql  &>> "$LOG_F" &
 
 tf="RML"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/get_last_elab_gas.sql)
 dtIN1=(${dt// / })
 
 if [[ $dtIN1 == "WARN:" || $dtIN1 == "" ]];then
  dtIN1=19990101
 fi

 echo "$tf : $dtIN1" &>> "$LOG_F"
 hive --hiveconf last_dt_elab_rml="$dtIN1" --hiveconf tipof="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/2_2_CreateTableParquetMisureGas_RML.sql  &>> "$LOG_F" &
 
 tf="VTG6"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/get_last_elab_gas.sql)
 dtIN2=(${dt// / })
  
  if [[ $dtIN2 == "WARN:" || $dtIN2 == "" ]];then
   dtIN2=19990101
  fi

 echo "$tf : $dtIN2" &>> "$LOG_F"
 hive --hiveconf last_dt_elab_vtg6="$dtIN2" --hiveconf tipof="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/2_3_CreateTableParquetMisureGas_VTG6.sql  &>> "$LOG_F" &
 
 tf="TAL"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/get_last_elab_gas.sql)
 dtIN3=(${dt// / })
 
 if [[ $dtIN3 == "WARN:" || $dtIN3 == "" ]];then
  dtIN3=19990101
 fi

 echo "$tf : $dtIN3" &>> "$LOG_F"
 hive --hiveconf last_dt_elab_tal="$dtIN3" --hiveconf tipof="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/2_4_CreateTableParquetMisureGas_TAL.sql  &>> "$LOG_F" &
 
 tf="TAV"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/get_last_elab_gas.sql)
 dtIN4=(${dt// / })
 
 if [[ $dtIN4 == "WARN:" || $dtIN4 == "" ]];then
  dtIN4=19990101
 fi

 echo "$tf : $dtIN4" &>> "$LOG_F"
 hive --hiveconf last_dt_elab_tav="$dtIN4" --hiveconf tipof="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/2_5_CreateTableParquetMisureGas_TAV.sql  &>> "$LOG_F" &
 
 tf="TGL"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/get_last_elab_gas.sql)
 dtIN5=(${dt// / })
 
 if [[ $dtIN5 == "WARN:" || $dtIN5 == "" ]];then
  dtIN5=19990101
 fi

 echo "$tf : $dtIN5" &>> "$LOG_F"
 hive --hiveconf last_dt_elab_tgl="$dtIN5" --hiveconf tipof="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/2_6_CreateTableParquetMisureGas_TGL.sql  &>> "$LOG_F" &
 
 tf="RGL"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/get_last_elab_gas.sql)
 dtIN6=(${dt// / })
 
 if [[ $dtIN6 == "WARN:" || $dtIN6 == "" ]];then
  dtIN6=19990101
 fi

 echo "$tf : $dtIN6" &>> "$LOG_F"
 hive --hiveconf last_dt_elab_rgl="$dtIN6" --hiveconf tipof="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/2_7_CreateTableParquetMisureGas_RGL.sql  &>> "$LOG_F" &
 
 tf="RMV"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/get_last_elab_gas.sql)
 dtIN7=(${dt// / })
 
 if [[ $dtIN7 == "WARN:" || $dtIN7 == "" ]];then
  dtIN7=19990101
 fi

 echo "$tf : $dtIN7" &>> "$LOG_F"
 hive --hiveconf last_dt_elab_rmv="$dtIN7" --hiveconf tipof="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/2_8_CreateTableParquetMisureGas_RMV.sql  &>> "$LOG_F" &


 tabelleh=( \
 PRT_CMG_TML_o \
 PRT_CMG_RML_o \
 PRT_CMG_TAL_o \
 PRT_CMG_TAV_o \
 PRT_CMG_TGL_o \
 PRT_CMG_RGL_o \
 PRT_CMG_RMV_o \
 )

 sleep 3

 ./check_exists_tbl.sh "CMG" ${tabelleh[@]} &>> "$LOG_F"

 tabelleh2=( \
 PRT_VTG6_o \
 )

 ./check_exists_tbl.sh "SWITCH_GAS" ${tabelleh2[@]} &>> "$LOG_F"
 

fi

if [[ $fase == "2" || $fase == "3" ]]
then

 echo "$(date) - ELABORAZIONE MISURE GAS" &>> "$LOG_F"
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_1_misure_gas_TML_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_2_misure_gas_RML_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_3_misure_gas_VTG6_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_4_misure_gas_TAL_Forniture_p.sql  &>> "$LOG_F" & 
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_5_misure_gas_TAV_Forniture_p.sql  &>> "$LOG_F" & 
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_6_misure_gas_TGL_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_7_misure_gas_RGL_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_8_misure_gas_RMV_Forniture_p.sql  &>> "$LOG_F" &

 tabelle=( \
 letture_tml \
 letture_rml \
 letture_vtg \
 letture_tal \
 letture_tav \
 letture_tgl \
 letture_rgl \
 letture_rmv \
 )

 sleep 3

 ./check_exists_tbl.sh "misuregas" ${tabelle[@]} &>> "$LOG_F"

 #AGGIORNAMENTO PARTITIONI RELATIVI AI NUOVI DATI ELABORATI
 ./update_partitions.sh "$LOG_F"
 
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_9_misure_gas_Volture_p.sql  &>> "$LOG_F" & 
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/3_9_misure_gas_Forniture_Consumi_p.sql  &>> "$LOG_F"

 echo "$(date) - OTTIMIZZAZIONE E MISURE ANTE SWITCHING" &>> "$LOG_F"
 ssh dmphclo14 "impala-shell -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/4_ImpalaRefresh.sql"  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/5_hql_fill_misuregas_switching.sql  &>> "$LOG_F"
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_DIFF/6_drop_tables.sql &>> "$LOG_F"
 echo "$(date) - OTTIMIZZAZIONE E MISURE ANTE SWITCHING COMPLETATO" &>> "$LOG_F"

fi

unset limit_gg_gas
unset HIVE_SKIP_SPARK_ASSEMBLY

./set_periodo/start_period.sh
limit_gg_gas=$(cat /mnt/isilonshare1/Software/EE/portale_consumi/set_periodo/.periodo)

annomese_limit=`date -d 'now - '"$limit_gg_gas"' days' '+%Y%m'`
echo "anno mese iniziale per drop partition $annomese_limit" &>> "$LOG_F"
hive -e "ALTER TABLE misuregas.misure_storic drop partition(annomese<$annomese_limit);" &>> "$LOG_F"
 
echo "completed" >> "$fcheck"

