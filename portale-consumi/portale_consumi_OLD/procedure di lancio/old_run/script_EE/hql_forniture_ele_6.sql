DROP TABLE IF EXISTS misure.prt_tmo_mv_f_o;

TRUNCATE TABLE misure.prt_tmo_mv_f;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT INTO misure.prt_tmo_mv_f PARTITION(annomese) 
                     SELECT CAST(eam AS DOUBLE)eam ,unix_timestamp(datarilevazione)d_rilevazione_number,
                     CAST(SUBSTR(regexp_replace(datarilevazione,'-',''),1,8) AS BIGINT) datarilevazione,
                     CAST(SUBSTR(regexp_replace(datavoltura,'-',''),1,8) AS BIGINT) data_voltura,
                     CAST(SUBSTR(regexp_replace(datavoltura,'-',''),7,2) AS INT)giorno_voltura,
                     SUBSTR(prt_tmo_mv.codice_pod,1,14)codice_pod,tipodato,
                     CAST(eaf1 AS DOUBLE)eaf1,CAST(eaf2 AS DOUBLE)eaf2,CAST(eaf3 AS DOUBLE)eaf3,cod_flusso,VALIDATO,
                     CAST(SUBSTR(regexp_replace(coalesce(d_upload,datarilevazione,datavoltura),'-',''),1,8) AS INT) d_upload,
                     CAST(SUBSTR(regexp_replace(datavoltura,'-',''),1,6) AS INT)annomese
                     FROM tmpod.prt_tmo_mv_p prt_tmo_mv left outer join tmpod.prt_tmo_file_p fl on prt_tmo_mv.n_id_file = fl.n_id_file
                     INNER JOIN (SELECT DISTINCT codice_pod FROM mongodbs.forniture) pods on pods.codice_pod = SUBSTR(prt_tmo_mv.codice_pod,1,14)
                     where  CAST(SUBSTR(regexp_replace(datavoltura,'-',''),1,6) AS INT) >=cast(concat(year(date_sub(current_date,396)),lpad(month(date_sub(current_date,396)),2,0)) as INT) 
                     AND (cod_flusso='RNV' OR (tipodato ='E' AND cod_flusso ='VNO' AND validato ='S'));

 CREATE TABLE misure.prt_tmo_mv_f_o
 (
   annomese int ,
   descr string
 )
 STORED AS PARQUET ;

