#!/bin/bash

LOG_F=$1
fcheck=$2
fase=$3
max_date_gas=$4


#IMPOSTAZIONE PERIODO MASSIMO PER GAS
./set_periodo/start_period.sh $max_date_gas
limit_gg_gas=$(cat /home/leonardo/AU_EE/portale_consumi/set_periodo/.periodo)

if [[ (! -v limit_gg_gas) || -z "$limit_gg_gas" ]];then
 limit_gg_gas=1126
 echo "Periodo massimo non impostato. Impostazione a 36 mesi di default" &>> "$LOG_F"
fi

export limit_gg_gas
echo "numero di giorni : $limit_gg_gas " &>> "$LOG_F"

rm -f "$fcheck"

if [[ $fase == "1" ]]
then
 echo "$(date) - ELABORAZIONE FORNITURE GAS" &>> "$LOG_F"
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/1_1_Gas_Residenza.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/1_2_Gas_DatiPrelievo.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/1_3_Gas_Misuratore.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/1_4_Gas_Distributori.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/1_5_Gas_Switch.sql  &>> "$LOG_F" &

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

 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/1_6_Gas_Forniture.sql  &>> "$LOG_F"

 #sleep 30

 echo "$(date) - PRELABORAZIONE MISURE GAS" &>> "$LOG_F"
 export HIVE_SKIP_SPARK_ASSEMBLY=true; 
 
 tf="TML"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/get_last_elab_gas.sql)
 dtIN=(${dt// / })
 echo "$tf : $dtIN" 
 hive -f --hiveconf last_dt_elab_tml="$dtIN" --hiveconf tipof="$tf" /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/2_1_CreateTableParquetMisureGas_TML.sql  &>> "$LOG_F" &
 
 tf="RML"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/get_last_elab_gas.sql)
 dtIN1=(${dt// / })
 echo "$tf : $dtIN1"
 hive -f --hiveconf last_dt_elab_rml="$dtIN1" --hiveconf tipof="$tf" /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/2_2_CreateTableParquetMisureGas_RML.sql  &>> "$LOG_F" &
 
 tf="VTG6"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/get_last_elab_gas.sql)
 dtIN2=(${dt// / })
 echo "$tf : $dtIN2"
 hive -f --hiveconf last_dt_elab_vtg6="$dtIN2" --hiveconf tipof="$tf" /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/2_3_CreateTableParquetMisureGas_VTG6.sql  &>> "$LOG_F" &
 
 tf="TAL"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/get_last_elab_gas.sql)
 dtIN3=(${dt// / })
 echo "$tf : $dtIN3"
 hive -f --hiveconf last_dt_elab_tal="$dtIN3" --hiveconf tipof="$tf" /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/2_4_CreateTableParquetMisureGas_TAL.sql  &>> "$LOG_F" &
 
 tf="TAV"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/get_last_elab_gas.sql)
 dtIN4=(${dt// / })
 echo "$tf : $dtIN4"
 hive -f --hiveconf last_dt_elab_tav="$dtIN4" --hiveconf tipof="$tf" /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/2_5_CreateTableParquetMisureGas_TAV.sql  &>> "$LOG_F" &
 
 tf="TGL"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/get_last_elab_gas.sql)
 dtIN5=(${dt// / })
 echo "$tf : $dtIN5"
 hive -f --hiveconf last_dt_elab_tgl="$dtIN5" --hiveconf tipof="$tf" /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/2_6_CreateTableParquetMisureGas_TGL.sql  &>> "$LOG_F" &
 
 tf="RGL"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/get_last_elab_gas.sql)
 dtIN6=(${dt// / })
 echo "$tf : $dtIN6"
 hive -f --hiveconf last_dt_elab_rgl="$dtIN6" --hiveconf tipof="$tf" /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/2_7_CreateTableParquetMisureGas_RGL.sql  &>> "$LOG_F" &
 
 tf="RMV"
 dt=$(hive --hiveconf tipo_flusso="$tf" -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/get_last_elab_gas.sql)
 dtIN7=(${dt// / })
 echo "$tf : $dtIN7"
 hive -f --hiveconf last_dt_elab_rmv="$dtIN7" --hiveconf tipof="$tf" /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/2_8_CreateTableParquetMisureGas_RMV.sql  &>> "$LOG_F" &


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

if [[ $fase == "2" ]]
then

 echo "$(date) - ELABORAZIONE MISURE GAS" &>> "$LOG_F"
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/3_1_misure_gas_TML_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/3_2_misure_gas_RML_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/3_3_misure_gas_VTG6_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/3_4_misure_gas_TGL_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/3_5_misure_gas_RGL_Forniture_p.sql  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/3_6_misure_gas_RMV_Forniture_p.sql  &>> "$LOG_F" &

 tabelle=( \
 letture_tml \
 letture_rml \
 letture_vtg \
 letture_tgl \
 letture_rgl \
 letture_rmv \
 )

 sleep 3

 ./check_exists_tbl.sh "misuregas" ${tabelle[@]} &>> "$LOG_F"

 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/3_7_misure_gas_TAV_TAL_Forniture_Consumi_p.sql  &>> "$LOG_F"

 echo "$(date) - OTTIMIZZAZIONE E MISURE ANTE SWITCHING" &>> "$LOG_F"
 ssh dmphclo14 "impala-shell -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/4_ImpalaRefresh.sql"  &>> "$LOG_F" &
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/5_hql_fill_misuregas_switching.sql  &>> "$LOG_F"
 hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_GAS_36/6_drop_tables.sql &>> "$LOG_F"
 echo "$(date) - OTTIMIZZAZIONE E MISURE ANTE SWITCHING COMPLETATO" &>> "$LOG_F"

fi

echo "completed" >> "$fcheck"

