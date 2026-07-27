#!/bin/bash

LOG_F=$1
LOG_F_GAS=$2
MAX_DATE_EE=$3
MAX_DATE_GAS=$4

fcheck_gas="/mnt/isilonshare1/Software/EE/portale_consumi/run_pc_gas.import"

#echo "$(date) - AGGIORNAMENTO DATI RCU" &> "$LOG_F"

#cd /mnt/isilonshare1/Software/EE/portale_consumi/sqoop
#/mnt/isilonshare1/Software/EE/portale_consumi/sqoop/import_from_rcu_new.sh "$LOG_F"
cd /mnt/isilonshare1/Software/EE/portale_consumi

echo "$(date) - AVVIO ELABORAZIONE FORNITURE GAS IN PARALLELO" &>> "$LOG_F"
./run_update_pc-gas36.sh "$LOG_F_GAS" "$fcheck_gas" "1" "$MAX_DATE_GAS" &


#IMPOSTAZIONE PERIODO MASSIMO PER EE
./set_periodo/start_period.sh $MAX_DATE_EE
limit_gg=$(cat /home/leonardo/AU_EE/portale_consumi/set_periodo/.periodo)

if [[ (! -v limit_gg) || -z "$limit_gg" ]];then
 limit_gg=1126
 echo "Periodo massimo non impostato. Impostazione a 36 mesi di default" &>> "$LOG_F"
fi

export limit_gg

nms=$(printf "%.0f" $(bc <<< "scale=2; $limit_gg/30"))
#nms=`expr $limit_gg + 0 `
if [ "$nms" -lt 38 ];then
 nms=`expr $nms + 1 `
fi

echo "numero di giorni : $limit_gg " &>> "$LOG_F"
echo "numero mesi : $nms" &>> "$LOG_F"

echo "$(date) - AGGIORNAMENTO FORNITURE,POD,PROCESSI,FASCE ELETTRICO" &>> "$LOG_F"
hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_EE_36/hql_forniture_ele_0.sql  &>> "$LOG_F" &
hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_EE_36/hql_forniture_ele_1.sql  &>> "$LOG_F" &
hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_EE_36/hql_forniture_ele_2.sql  &>> "$LOG_F" &
hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_EE_36/hql_forniture_ele_3.sql  &>> "$LOG_F" &
hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_EE_36/hql_forniture_ele_4.sql  &>> "$LOG_F" &

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


hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_EE_36/hql_forniture_ele_7.sql  &>> "$LOG_F"

unset limit_gg

#verifico che il primo step del GAS sia completo
./test.sh "$fcheck_gas" &>> "$LOG_F"


echo "$(date) - AVVIO ELABORAZIONE MISURE GAS IN PARALLELO" &>> "$LOG_F"
./run_update_pc-gas36.sh "$LOG_F_GAS" "$fcheck_gas" "2" "$MAX_DATE_GAS" &

echo "$(date) - ELABORAZIONE MISURE ELETTRICHE"  &>> "$LOG_F"
cd /mnt/isilonshare1/Software/EE/bin

rm -f ricerca_pod_orari.txt
echo "$(date) - RICERCA POD TRATTAMENTO ORARIO IN BCK"  &>> ricerca_pod_orari.txt
#riceca pod con trattamento orario in background
./flusso-misure-pc2.sh -PC_MS TO -nm "$nms" &>> ricerca_pod_orari.txt &
PID_TO=$!

rm -f misure_non_orarie.txt
echo "$(date) - ELABORAZIONE MISURE NON ORARIE IN BCK"  &>> misure_non_orarie.txt
#elaborazione misure non orarie in background
./flusso-misure-pc2.sh -PC_MS ST-NO -nm "$nms" &>> misure_non_orarie.txt &
PID_NO=$!

echo "$(date) - IN ATTESA COMPLETAMENTO RICERCA TRATTAMENTO ORARIO"  &>> "$LOG_F"
wait $PID_TO
cat ricerca_pod_orari.txt &>> "$LOG_F"

#elaborazione misure orarie
echo "$(date) - ELABORAZIONE MISURE ORARIE"  &>> "$LOG_F"
./flusso-misure-pc.sh -PC_MS ST-O -nm "$nms" &>> "$LOG_F"

echo "$(date) - IN ATTESA COMPLETAMENTO MISURE NON ORARIE"  &>> "$LOG_F"
wait $PID_NO
cat misure_non_orarie.txt &>> "$LOG_F"

#elaborazione misure da non orarie ad orarie
echo "$(date) - ELABORAZIONE MISURE DA NON ORARIE A ORARIE"  &>> "$LOG_F"
./flusso-misure-pc.sh -PC_MS ON -nm "$nms" &>> "$LOG_F"

rm -f consumi_non_orari.txt
echo "$(date) - CALCOLO CONSUMI NON ORARI IN BCK"  &>> consumi_non_orari.txt
./flusso-misure-pc2.sh -PC_MS CN -nm "$nms" &>> consumi_non_orari.txt &
PID_CNO=$!

#elaborazione consumi orari e non orari
echo "$(date) - CALCOLO CONSUMI ORARI"  &>> "$LOG_F"
./flusso-misure-pc.sh -PC_MS CO-MXM -nm "$nms" &>> "$LOG_F"

cd /mnt/isilonshare1/Software/EE/portale_consumi


echo "$(date) - IN ATTESA COMPLETAMENTO CALCOLO CONSUMI NON ORARI"  &>> "$LOG_F"
wait $PID_CNO
cat /mnt/isilonshare1/Software/EE/bin/consumi_non_orari.txt &>> "$LOG_F"

echo "$(date) - ELABORAZIONE MISURE ANTE SWITCHING"  &>> "$LOG_F"
hive -f /mnt/isilonshare1/Software/EE/portale_consumi/script_EE_36/hql_fill_misure_switching.sql &>> "$LOG_F"
echo "$(date) - ELABORAZIONE MISURE ANTE SWITCHING COMPLETATO"  &>> "$LOG_F"
 
./test.sh "$fcheck_gas" &>> "$LOG_F"


echo "$(date) - CARICAMENTO DATI SU MONGODB"  &>> "$LOG_F"
cd /mnt/isilonshare1/Software/EE/bin

./flusso-misure-pc_mongo.sh -PC_EX FG &>> "$LOG_F"
./flusso-misure-pc_mongo.sh -PC_EX F &>> "$LOG_F"
./flusso-misure-pc_mongo.sh -PC_EX MG-SPLIT33-I &>> "$LOG_F"
./flusso-misure-pc_mongo.sh -PC_EX MG-SPLIT3-I &>> "$LOG_F"
./flusso-misure-pc_mongo.sh -PC_EX M-SPLIT3-I &>> "$LOG_F"
./flusso-misure-pc_mongo.sh -PC_EX M-SPLIT33-I &>> "$LOG_F"
./flusso-misure-pc_mongo.sh -PC_EX I -l &>> "$LOG_F" &

cd /mnt/isilonshare1/Software/EE/portale_consumi
echo "$(date) - AVVIO ELABORAZIONE MISURE STORICHE"  &>> "$LOG_F"
./run_storic_pc.sh "$nms"
echo "$(date) - ELABORAZIONE MISURE STORICHE COMPLETATA"  &>> "$LOG_F"

