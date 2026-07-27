DROP TABLE IF EXISTS misure.prt_tmo_mn_f_o;

TRUNCATE TABLE misure.prt_tmo_mn_f;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT INTO misure.prt_tmo_mn_f PARTITION(annomese) 
                     SELECT CAST(eam AS DOUBLE)eam ,
                     CAST(SUBSTR(regexp_replace(d_ricezione,'-',''),1,8) AS BIGINT) data_ricezione,
                     CAST(SUBSTR(regexp_replace(datamisura,'-',''),1,8) AS BIGINT) data_lettura,
                     CAST(giornomisura AS INT)giornomisura,
                     SUBSTR(prt_tmo_mn.codice_pod,1,14)codice_pod,tipodato,CAST(perdita AS DOUBLE)perdita,
                     CAST(eaf1 AS DOUBLE)eaf1,CAST(eaf2 AS DOUBLE)eaf2,CAST(eaf3 AS DOUBLE)eaf3,cod_flusso,motivazione,validato,
                     CAST(SUBSTR(regexp_replace(nvl(d_upload,d_ricezione),'-',''),1,8) AS INT) d_upload,
                     CAST(annomese AS INT)annomese
                     FROM tmpod.prt_tmo_mn_p prt_tmo_mn left outer join tmpod.prt_tmo_file_p fl on prt_tmo_mn.n_id_file = fl.n_id_file
                     INNER JOIN (SELECT DISTINCT codice_pod FROM mongodbs.forniture) pods on pods.codice_pod = SUBSTR(prt_tmo_mn.codice_pod,1,14)
                     where  CAST(SUBSTR(regexp_replace(datamisura,'-',''),1,6) AS INT) >=cast(concat(year(date_sub(current_date,${env:limit_gg})),lpad(month(date_sub(current_date,${env:limit_gg})),2,0)) as INT)
                     AND (cod_flusso='RNO' OR (tipodato ='E' AND cod_flusso ='PNO' AND validato ='S'));

 CREATE TABLE misure.prt_tmo_mn_f_o
 (
   annomese int ,
   descr string
 )
 STORED AS PARQUET ;
