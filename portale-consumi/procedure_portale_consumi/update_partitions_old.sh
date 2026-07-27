#!/bin/bash

LOG_F=$1

delete_parts () {

    eval str_partitions=$1
    eval tipoflusso=$2


    IFS=' ' 
    read -ra ADDR <<< "$str_partitions" 
    for i in "${ADDR[@]}"; do
        if [[ "$i" == "WARN:" ]];then
         exit
        fi
      hive -e "ALTER TABLE misuregas.misure_storic drop if exists partition(tipo_flusso='$tipoflusso',annomese="$i");" &>> "$LOG_F"
    done
}

echo "ESTRAZIONE PARTIZIONI TML" &>> "$LOG_F"
parts_tml=$(hive -e "SELECT DISTINCT annomese_riferimento from misuregas.letture_tml;")
echo "$part_tml" &>> "$LOG_F"

echo "ESTRAZIONE PARTIZIONI RML" &>> "$LOG_F"
parts_rml=$(hive -e "SELECT DISTINCT annomese_riferimento from misuregas.letture_rml;")
echo "$part_rml" &>> "$LOG_F"

echo "ESTRAZIONE PARTIZIONI VTG6" &>> "$LOG_F"
parts_vtg6=$(hive -e "SELECT DISTINCT annomese_riferimento from misuregas.letture_vtg;")
echo "$part_vtg6" &>> "$LOG_F"

echo "ESTRAZIONE PARTIZIONI TGL" &>> "$LOG_F"
parts_tgl=$(hive -e "SELECT DISTINCT mese_comp annomese_riferimento from misuregas.letture_tgl;")
echo "$part_tgl" &>> "$LOG_F"

echo "ESTRAZIONE PARTIZIONI RGL" &>> "$LOG_F"
parts_rgl=$(hive -e "SELECT DISTINCT mese_comp annomese_riferimento from misuregas.letture_rgl;")
echo "$part_rgl" &>> "$LOG_F"

echo "ESTRAZIONE PARTIZIONI RMV" &>> "$LOG_F"
parts_rmv=$(hive -e "SELECT DISTINCT annomese_riferimento from misuregas.letture_rmv;")
echo "$part_rmv" &>> "$LOG_F"

echo "ESTRAZIONE PARTIZIONI TAL" &>> "$LOG_F"
parts_tal=$(hive -e "SELECT DISTINCT annomese_riferimento from misuregas.letture_tal;")
echo "$part_tal" &>> "$LOG_F"

echo "ESTRAZIONE PARTIZIONI TAV" &>> "$LOG_F"
parts_tav=$(hive -e "SELECT DISTINCT annomese_riferimento from misuregas.letture_tav;")
echo "$part_tav" &>> "$LOG_F"

echo "ELIMINAZIONE PARTIZIONI TML" &>> "$LOG_F"
delete_parts "\${parts_tml}" "TML"

echo "ELIMINAZIONE PARTIZIONI RML" &>> "$LOG_F"
delete_parts "\${parts_rml}" "RML"

echo "ELIMINAZIONE PARTIZIONI VTG6" &>> "$LOG_F"
delete_parts "\${parts_vtg6}" "VTG6"

echo "ELIMINAZIONE PARTIZIONI TGL" &>> "$LOG_F"
delete_parts "\${parts_tgl}" "TGL"

echo "ELIMINAZIONE PARTIZIONI RGL" &>> "$LOG_F"
delete_parts "\${parts_rgl}" "RGL"

echo "ELIMINAZIONE PARTIZIONI RMV" &>> "$LOG_F"
delete_parts "\${parts_rmv}" "RMV"

echo "ELIMINAZIONE PARTIZIONI TAL" &>> "$LOG_F"
delete_parts "\${parts_tal}" "TAL"

echo "ELIMINAZIONE PARTIZIONI TAV" &>> "$LOG_F"
delete_parts "\${parts_tav}" "TAV"

echo "AVVIO AGGIORNAMENTO PARTIZIONI IN misuregas.misure_storic" &>> "$LOG_F"

hive -e "
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO misuregas.misure_storic PARTITION(tipo_flusso,annomese)
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,'' data_lettura,data_racc,tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,cast(CONCAT(substr(data_racc,1,4),substr(data_racc,6,2)) as int) annomese
from misuregas.letture_tml 
UNION ALL 
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,'' data_lettura,data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,cast(CONCAT(substr(data_racc,1,4),substr(data_racc,6,2)) as int) annomese
from misuregas.letture_rml 
UNION ALL
select  cod_pdr,cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,'' data_lettura,data_racc,tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_vtg 
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,data_lettura,'' data_racc,'' tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,CONCAT(YEAR(data_lettura),LPAD(MONTH( data_lettura),2,0)) annomese
from misuregas.letture_tal 
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,data_lettura,'' data_racc,'' tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,CONCAT(YEAR(data_lettura),LPAD(MONTH( data_lettura),2,0)) annomese
from misuregas.letture_tav   
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,mese_comp annomese_riferimento, mese_comp,
dt_caricamento,let_tot_prel, data_comp,'' data_lettura,'' data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,CONCAT(YEAR(data_comp),LPAD(MONTH( data_comp),2,0)) annomese
from misuregas.letture_tgl   
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,mese_comp annomese_riferimento, mese_comp,
dt_caricamento,let_tot_prel, data_comp,'' data_lettura,'' data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,CONCAT(YEAR(data_comp),LPAD(MONTH( data_comp),2,0)) annomese
from misuregas.letture_rgl 
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel, data_comp,'' data_lettura,'' data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_rmv ;

DROP TABLE IF EXISTS misuregas.letture_tml;
DROP TABLE IF EXISTS misuregas.letture_rml;
DROP TABLE IF EXISTS misuregas.letture_vtg;
DROP TABLE IF EXISTS misuregas.letture_tal;
DROP TABLE IF EXISTS misuregas.letture_tav;
DROP TABLE IF EXISTS misuregas.letture_tgl;
DROP TABLE IF EXISTS misuregas.letture_rgl;
DROP TABLE IF EXISTS misuregas.letture_rmv;

" &>> "$LOG_F"

echo "AGGIORNAMENTO PARTIZIONI IN misuregas.misure_storic COMPLETATO" &>> "$LOG_F"
