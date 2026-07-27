#!/bin/bash

LOG_F=$1

update_parts () {

    eval str_partitions=$1
    eval tipoflusso=$2


    IFS=' ' 
    read -ra ADDR <<< "$str_partitions" 
    for i in "${ADDR[@]}"; do
        if [[ "$i" == "WARN:" ]];then
         exit
        fi
      hive -e "set hive.exec.dynamic.partition.mode=nonstrict;
               ALTER TABLE misuregas.last_elab_gas DROP IF EXISTS PARTITION(tipoflusso='$tipoflusso');
               insert into misuregas.last_elab_gas PARTITION(tipoflusso) 
               SELECT date_format(current_date(),'yyyyMMdd') annomesegiornoelab , '$tipoflusso' tipoflusso; " &>> "$LOG_F"

    done
}

echo "AVVIO ESTRAZIONE PARTIZIONI NUOVE MISURE" &>> "$LOG_F"

hive -e "
DROP TABLE IF EXISTS letture_tml_annomese;
CREATE TABLE letture_tml_annomese as
select DISTINCT cast(CONCAT(substr(data_racc,1,4),substr(data_racc,6,2)) as int) AM FROM misuregas.letture_tml; 

DROP TABLE IF EXISTS letture_rml_annomese;
CREATE TABLE letture_rml_annomese as
select DISTINCT cast(CONCAT(substr(data_racc,1,4),substr(data_racc,6,2)) as int) AM FROM misuregas.letture_rml; 

DROP TABLE IF EXISTS letture_vtg_annomese;
CREATE TABLE letture_vtg_annomese as
select DISTINCT annomese_riferimento AM FROM misuregas.letture_vtg; 

DROP TABLE IF EXISTS letture_tal_annomese;
CREATE TABLE letture_tal_annomese as
select DISTINCT cast(CONCAT(YEAR(data_lettura),LPAD(MONTH( data_lettura),2,0)) as int) AM FROM misuregas.letture_tal; 

DROP TABLE IF EXISTS letture_tav_annomese;
CREATE TABLE letture_tav_annomese as
select DISTINCT cast(CONCAT(YEAR(data_lettura),LPAD(MONTH( data_lettura),2,0)) as int) AM FROM misuregas.letture_tav; 

DROP TABLE IF EXISTS letture_tgl_annomese;
CREATE TABLE letture_tgl_annomese as
select DISTINCT cast(CONCAT(YEAR(data_comp),LPAD(MONTH( data_comp),2,0)) as int) AM FROM misuregas.letture_tgl; 

DROP TABLE IF EXISTS letture_rgl_annomese;
CREATE TABLE letture_rgl_annomese as
select DISTINCT cast(CONCAT(YEAR(data_comp),LPAD(MONTH( data_comp),2,0)) as int) AM FROM misuregas.letture_rgl; 

DROP TABLE IF EXISTS letture_rmv_annomese;
CREATE TABLE letture_rmv_annomese as
select DISTINCT annomese_riferimento AM FROM misuregas.letture_rmv;
"

echo "AVVIO AGGIORNAMENTO PARTIZIONI IN misuregas.misure_storic" &>> "$LOG_F"

hive -e "
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO misuregas.misure_storic PARTITION(tipo_flusso,annomese)
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,'' data_lettura,data_racc,tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,cast(CONCAT(substr(data_racc,1,4),substr(data_racc,6,2)) as int) annomese
from misuregas.letture_tml src
where CONCAT(src.data_racc,src.cod_pdr,src.codice_fornitura) NOT IN 
 (SELECT CONCAT(misure_storic.data_racc,misure_storic.cod_pdr,misure_storic.codice_fornitura)KK 
  FROM misuregas.misure_storic INNER JOIN letture_tml_annomese A ON annomese=AM WHERE tipo_flusso='TML')
UNION ALL 
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,'' data_lettura,data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,cast(CONCAT(substr(data_racc,1,4),substr(data_racc,6,2)) as int) annomese
from misuregas.letture_rml src
where CONCAT(src.data_racc,src.cod_pdr,src.codice_fornitura) NOT IN 
 (SELECT CONCAT(misure_storic.data_racc,misure_storic.cod_pdr,misure_storic.codice_fornitura)KK 
  FROM misuregas.misure_storic INNER JOIN letture_rml_annomese A ON annomese=AM WHERE tipo_flusso='RML')  
UNION ALL
select  cod_pdr,cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,'' data_lettura,data_racc,tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_vtg src
where CONCAT(src.data_racc,src.cod_pdr,src.codice_fornitura) NOT IN 
 (SELECT CONCAT(misure_storic.data_racc,misure_storic.cod_pdr,misure_storic.codice_fornitura)KK 
  FROM misuregas.misure_storic INNER JOIN letture_vtg_annomese A ON annomese=AM WHERE tipo_flusso='VTG6') 
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,data_lettura,'' data_racc,'' tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,CONCAT(YEAR(data_lettura),LPAD(MONTH( data_lettura),2,0)) annomese
from misuregas.letture_tal src
where CONCAT(src.data_lettura,src.cod_pdr,src.codice_fornitura) NOT IN 
 (SELECT CONCAT(misure_storic.data_lettura,misure_storic.cod_pdr,misure_storic.codice_fornitura)KK 
  FROM misuregas.misure_storic INNER JOIN letture_tal_annomese A ON annomese=AM WHERE tipo_flusso='TAL') 
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,data_lettura,'' data_racc,'' tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,CONCAT(YEAR(data_lettura),LPAD(MONTH(data_lettura),2,0)) annomese
from misuregas.letture_tav src
where CONCAT(src.data_lettura,src.cod_pdr,src.codice_fornitura) NOT IN 
 (SELECT CONCAT(misure_storic.data_lettura,misure_storic.cod_pdr,misure_storic.codice_fornitura)KK 
  FROM misuregas.misure_storic INNER JOIN letture_tav_annomese A ON annomese=AM WHERE tipo_flusso='TAV')  
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,mese_comp annomese_riferimento, mese_comp,
dt_caricamento,let_tot_prel, data_comp,'' data_lettura,'' data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,CONCAT(YEAR(data_comp),LPAD(MONTH( data_comp),2,0)) annomese
from misuregas.letture_tgl src  
where CONCAT(src.data_comp,src.cod_pdr,src.codice_fornitura) NOT IN 
 (SELECT CONCAT(misure_storic.data_comp,misure_storic.cod_pdr,misure_storic.codice_fornitura)KK 
  FROM misuregas.misure_storic INNER JOIN letture_tgl_annomese A ON annomese=AM WHERE tipo_flusso='TGL')  
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,mese_comp annomese_riferimento, mese_comp,
dt_caricamento,let_tot_prel, data_comp,'' data_lettura,'' data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,CONCAT(YEAR(data_comp),LPAD(MONTH( data_comp),2,0)) annomese
from misuregas.letture_rgl src
where CONCAT(src.data_comp,src.cod_pdr,src.codice_fornitura) NOT IN 
 (SELECT CONCAT(misure_storic.data_comp,misure_storic.cod_pdr,misure_storic.codice_fornitura)KK 
 FROM misuregas.misure_storic INNER JOIN letture_rgl_annomese A ON annomese=AM WHERE tipo_flusso='RGL' )  
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel, data_comp,'' data_lettura,'' data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_rmv src
where CONCAT(src.data_comp,src.cod_pdr,src.codice_fornitura) NOT IN 
 (SELECT CONCAT(misure_storic.data_comp,misure_storic.cod_pdr,misure_storic.codice_fornitura)KK 
  FROM misuregas.misure_storic INNER JOIN letture_rmv_annomese A ON annomese=AM WHERE tipo_flusso='RMV' );

DROP TABLE IF EXISTS misuregas.letture_tml;
DROP TABLE IF EXISTS misuregas.letture_rml;
DROP TABLE IF EXISTS misuregas.letture_vtg;
DROP TABLE IF EXISTS misuregas.letture_tal;
DROP TABLE IF EXISTS misuregas.letture_tav;
DROP TABLE IF EXISTS misuregas.letture_tgl;
DROP TABLE IF EXISTS misuregas.letture_rgl;
DROP TABLE IF EXISTS misuregas.letture_rmv;

DROP TABLE IF EXISTS letture_tml_annomese;
DROP TABLE IF EXISTS letture_rml_annomese;
DROP TABLE IF EXISTS letture_vtg_annomese;
DROP TABLE IF EXISTS letture_tal_annomese;
DROP TABLE IF EXISTS letture_tav_annomese;
DROP TABLE IF EXISTS letture_tgl_annomese;
DROP TABLE IF EXISTS letture_rgl_annomese;
DROP TABLE IF EXISTS letture_rmv_annomese;
" &>> "$LOG_F"

echo "AGGIORNAMENTO PARTIZIONE ULTIMA ELABORAZIONE TML" &>> "$LOG_F"
update_parts "\${parts_tml}" "TML"

echo "AGGIORNAMENTO PARTIZIONE ULTIMA ELABORAZIONE RML" &>> "$LOG_F"
update_parts "\${parts_rml}" "RML"

echo "AGGIORNAMENTO PARTIZIONE ULTIMA ELABORAZIONE VTG6" &>> "$LOG_F"
update_parts "\${parts_vtg6}" "VTG6"

echo "AGGIORNAMENTO PARTIZIONE ULTIMA ELABORAZIONE TGL" &>> "$LOG_F"
update_parts "\${parts_tgl}" "TGL"

echo "AGGIORNAMENTO PARTIZIONE ULTIMA ELABORAZIONE RGL" &>> "$LOG_F"
update_parts "\${parts_rgl}" "RGL"

echo "AGGIORNAMENTO PARTIZIONE ULTIMA ELABORAZIONE RMV" &>> "$LOG_F"
update_parts "\${parts_rmv}" "RMV"

echo "AGGIORNAMENTO PARTIZIONE ULTIMA ELABORAZIONE TAL" &>> "$LOG_F"
update_parts "\${parts_tal}" "TAL"

echo "AGGIORNAMENTO PARTIZIONE ULTIMA ELABORAZIONE TAV" &>> "$LOG_F"
update_parts "\${parts_tav}" "TAV"

echo "AGGIORNAMENTO PARTIZIONI IN misuregas.misure_storic COMPLETATO" &>> "$LOG_F"
