#!/bin/bash

LOG_F=$1

echo "$(date) - AGGIORNAMENTO DATI RCU" &> "$LOG_F"

cd /home/leonardo/portale_consumi/sqoop
/home/leonardo/portale_consumi/sqoop/import_from_rcu_new.sh "$LOG_F"
cd /home/leonardo/portale_consumi

echo "$(date) - AGGIORNAMENTO FORNITURE,POD,PROCESSI,FASCE ELETTRICO" &>> "$LOG_F"
hive -f /home/leonardo/portale_consumi/script_EE/hql_forniture_ele_0.sql  &>> "$LOG_F" & 
hive -f /home/leonardo/portale_consumi/script_EE/hql_forniture_ele_1.sql  &>> "$LOG_F" & 
hive -f /home/leonardo/portale_consumi/script_EE/hql_forniture_ele_2.sql  &>> "$LOG_F" & 
hive -f /home/leonardo/portale_consumi/script_EE/hql_forniture_ele_3.sql  &>> "$LOG_F" & 
hive -f /home/leonardo/portale_consumi/script_EE/hql_forniture_ele_4.sql  &>> "$LOG_F" & 

tabelle=( \
forniture \
forniture_info \
switch \
fasce \
gdm \
RCU_POD_DISTR \
)

sleep 3

./check_exists_tbl.sh "mongodbs" ${tabelle[@]} &>> "$LOG_F" 

hive -f /home/leonardo/portale_consumi/script_EE/hql_forniture_ele_5.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_EE/hql_forniture_ele_6.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_EE/hql_forniture_ele_7.sql  &>> "$LOG_F"

tabelle=( \
prt_tmo_mv_f_o \
prt_tmo_mn_f_o \
)

sleep 3

./check_exists_tbl.sh "misure" ${tabelle[@]} &>> "$LOG_F"

echo "$(date) - ELABORAZIONE MISURE ELETTRICHE"  &>> "$LOG_F"
cd /home/leonardo/last_release/bin/
./flusso-misure-pc.sh -PC_MS ST-TO &>> "$LOG_F"
./flusso-misure-pc.sh -PC_MS N &>> "$LOG_F"
./flusso-misure-pc.sh -PC_MS O &>> "$LOG_F"

cd /home/leonardo/portale_consumi


echo "$(date) - ELABORAZIONE STORICO ELETTRICHE"  &>> "$LOG_F"
hive -f /home/leonardo/portale_consumi/script_EE/hql_storic_ele.sql  &>> "$LOG_F" &

echo "$(date) - ELABORAZIONE MISURE ANTE SWITCHING"  &>> "$LOG_F"
hive -f /home/leonardo/portale_consumi/script_EE/hql_fill_misure_switching.sql &>> "$LOG_F" &

echo "$(date) - ELABORAZIONE FORNITURE GAS" &>> "$LOG_F"
hive -f /home/leonardo/portale_consumi/script_GAS/1_queryGasForniture_p.sql  &>> "$LOG_F"

echo "$(date) - ELABORAZIONE MISURE GAS" &>> "$LOG_F"
hive -f /home/leonardo/portale_consumi/script_GAS/2_CreateTableParquetMisureGasOttimizzate_1.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/2_CreateTableParquetMisureGasOttimizzate_2.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/2_CreateTableParquetMisureGasOttimizzate_3.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/2_CreateTableParquetMisureGasOttimizzate_4.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/2_CreateTableParquetMisureGasOttimizzate_5.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/2_CreateTableParquetMisureGasOttimizzate_6.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/2_CreateTableParquetMisureGasOttimizzate_7.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/2_CreateTableParquetMisureGasOttimizzate_8.sql  &>> "$LOG_F" &


tabelle=( \
PRT_CMG_TML_o \
PRT_CMG_RML_o \
PRT_CMG_TAL_o \
PRT_CMG_TAV_o \
PRT_CMG_TGL_o \
PRT_CMG_RGL_o \
PRT_CMG_RMV_o \
)

sleep 3

./check_exists_tbl.sh "CMG" ${tabelle[@]} &>> "$LOG_F"

tabelle=( \
PRT_VTG6_o \
)

./check_exists_tbl.sh "SWITCH_GAS" ${tabelle[@]} &>> "$LOG_F"


hive -f /home/leonardo/portale_consumi/script_GAS/3_misure_gasDaniele5ottimizzato_p_1.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/3_misure_gasDaniele5ottimizzato_p_2.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/3_misure_gasDaniele5ottimizzato_p_3.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/3_misure_gasDaniele5ottimizzato_p_4.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/3_misure_gasDaniele5ottimizzato_p_5.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/3_misure_gasDaniele5ottimizzato_p_6.sql  &>> "$LOG_F" &

tabelle=( \
letture_tml \
letture_rml \
letture_vtg \
letture_tgl \
letture_rgl \
letture_rmv \
)

sleep 3

./check_exists_tbl.sh "au_test" ${tabelle[@]} &>> "$LOG_F"

hive -f /home/leonardo/portale_consumi/script_GAS/3_misure_gasDaniele5ottimizzato_p_7.sql  &>> "$LOG_F"
impala-shell -f /home/leonardo/portale_consumi/script_GAS/4_ImpalaRefresh.sql  &>> "$LOG_F" &
hive -f /home/leonardo/portale_consumi/script_GAS/5_hql_fill_misuregas_switching.sql  &>> "$LOG_F"
hive -f /home/leonardo/portale_consumi/script_GAS/6_drop_tables.sql &>> "$LOG_F"

echo "$(date) - CARICAMENTO DATI SU MONGODB"  &>> "$LOG_F"
cd /home/leonardo/last_release/bin/
./flusso-misure-pc.sh -PC_EX FG  &>> "$LOG_F"
./flusso-misure-pc.sh -PC_EX F  &>> "$LOG_F"
./flusso-misure-pc.sh -PC_EX MG  &>> "$LOG_F"
./flusso-misure-pc.sh -PC_EX M  &>> "$LOG_F"
./flusso-misure-pc.sh -PC_EX I -l &>> "$LOG_F"







