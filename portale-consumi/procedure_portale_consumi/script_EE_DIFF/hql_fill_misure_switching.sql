set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

ALTER TABLE misure.misure_mensili_c DROP IF EXISTS PARTITION(da_antswitch='1');
ALTER TABLE misure.misure_non_orarie_c DROP IF EXISTS PARTITION(da_antswitch='1');

MSCK REPAIR TABLE misure.misure_mensili_c;
MSCK REPAIR TABLE misure.misure_non_orarie_c;

drop table if exists misure.tbl_forniture;

CREATE TABLE IF NOT EXISTS misure.tbl_forniture STORED AS PARQUET AS
SELECT distinct t_cf cf_piva ,codice_fornitura n_id_fornitura,codice_pod,
CAST(data_inizio_fornitura_num AS BIGINT)inizio,CAST(data_fine_fornitura_num AS BIGINT) fine
from mongodbs.forniture_elettriche  DISTRIBUTE BY codice_pod;


--INSERIMENTO MISURE MANCANTI NEL MESE DI INIZIO FORNITURA
--lA SCRITTURA AVVIENE SULLA TABELLA DELLE MISURE NON ORARIE POICHE
--LA PROCEDURA DI SCRITTURA SU MONHODB LI RIPORTA ANCHE NELLE MISURE MENSILI*/

--SET hive.auto.convert.join=false;

drop table if exists misure.forniture_ee_iniziali_senza_misure;

CREATE TABLE IF NOT EXISTS misure.forniture_ee_iniziali_senza_misure STORED AS PARQUET 
as
SELECT KK_KEY , t_cf,codice_pod,codice_fornitura,annomese_iniziof from
(
select CONCAT(FF.cf_piva,FF.codice_pod,FF.n_id_fornitura) KK_KEY  , cf_piva t_cf,codice_pod,n_id_fornitura codice_fornitura,
cast(substr(cast(FF.inizio as string),1,6) as int)annomese_iniziof
from misure.tbl_forniture FF
where inizio <> 0
) FO
left outer join misure.misure_non_orarie_c M_NORA
ON CONCAT(FO.KK_KEY,FO.annomese_iniziof)=CONCAT(M_NORA.cf_piva,M_NORA.pod,M_NORA.n_id_fornitura,M_NORA.competenza_consumi)
LEFT OUTER JOIN misure.misure_mensili_c M_ORA
ON CONCAT(FO.KK_KEY,FO.annomese_iniziof)=CONCAT(M_ORA.cf_piva,M_ORA.pod,M_ORA.n_id_fornitura,M_ORA.competenza_consumi)
WHERE M_NORA.competenza_consumi IS NULL AND  M_ORA.competenza_consumi IS NULL ;


INSERT INTO misure.misure_non_orarie_c PARTITION(da_antswitch)
select t_cf cf_piva  ,codice_fornitura n_id_fornitura ,annomese_iniziof competenza_consumi ,
codice_pod pod ,'' tipo_misura ,
null lettura_misura_monoraria ,null lettura_misura_f1 ,null lettura_misura_f2 ,
null lettura_misura_f3 ,null lettura_misura_f4 ,null lettura_misura_f5 ,
null lettura_misura_f6 ,null delta_misure_f1 ,null delta_misure_f2 ,
null delta_misure_f3 ,null delta_misure_f4 ,null delta_misure_f5 ,
null delta_misure_f6 ,null delta_misura_monoraria ,'' tipo_flusso2 ,null data_lettura,
null potf1,null potf2,null potf3,null potm,'1' da_antswitch
from misure.forniture_ee_iniziali_senza_misure;

drop table if exists misure.forniture_ee_iniziali_senza_misure;



--misure ante switching
DROP TABLE IF EXISTS misure.forniture_switching ;

CREATE TABLE misure.forniture_switching STORED AS PARQUET 
as
select CONCAT(FF.cf_piva,FF.codice_pod,FF.n_id_fornitura) KK_KEY  , FF.fine data_fine_fornitura_num,
cast(substr(cast(FF.fine as string),1,6) as int)annomese_finef ,
max(cast(substr(cast(FF.fine as string),1,6) as int))  over ( partition by FF.cf_piva,FF.codice_pod) max_annomese_fornitura
from misure.tbl_forniture FF
WHERE CONCAT(FF.cf_piva,FF.codice_pod) IN
 (SELECT CONCAT(FF_2.cf_piva,FF_2.codice_pod)
  from misure.tbl_forniture FF_2
  group by FF_2.cf_piva,FF_2.codice_pod having count(*) > 1);


INSERT INTO misure.misure_non_orarie_c PARTITION(da_antswitch)
select cf_piva  ,n_id_fornitura ,annomese_finef competenza_consumi ,
pod ,tipo_misura ,
null lettura_misura_monoraria ,null lettura_misura_f1 ,null lettura_misura_f2 ,
null lettura_misura_f3 ,null lettura_misura_f4 ,null lettura_misura_f5 ,
null lettura_misura_f6 ,null delta_misure_f1 ,null delta_misure_f2 ,
null delta_misure_f3 ,null delta_misure_f4 ,null delta_misure_f5 ,
null delta_misure_f6 ,null delta_misura_monoraria ,CONCAT('SW_',tipo_flusso2)tipo_flusso2 ,data_fine_fornitura_num data_lettura,
null potf1,null potf2,null potf3,null potm,'1' da_antswitch
from
(
select *
from
(SELECT cf_piva ,n_id_fornitura,competenza_consumi,pod ,tipo_misura,tipo_flusso2, CONCAT(cf_piva,pod,n_id_fornitura)KK_MS,MAX(competenza_consumi) over ( partition by cf_piva,pod,n_id_fornitura) max_misura_fornitura from misure.misure_non_orarie_c where data_lettura is not null
) MS
inner join misure.forniture_switching  forniture
ON KK_MS = KK_KEY
WHERE max_misura_fornitura <> annomese_finef
and annomese_finef <> max_annomese_fornitura
)TF where max_misura_fornitura = competenza_consumi
and CONCAT(TF.cf_piva,TF.pod,TF.n_id_fornitura,TF.annomese_finef) not in
(SELECT CONCAT(misure_mensili_c.cf_piva,misure_mensili_c.pod,misure_mensili_c.n_id_fornitura,misure_mensili_c.competenza_consumi)xx
FROM misure.misure_mensili_c );

INSERT INTO misure.misure_mensili_c PARTITION(competenza_consumi,da_antswitch)
SELECT cf_piva  ,n_id_fornitura ,tipo_misura ,
null lettura_misura_monoraria ,null delta_misura_monoraria ,
null lettura_misura_f1 ,null lettura_misura_f2 ,
null lettura_misura_f3 ,null lettura_misura_f4 ,null lettura_misura_f5 ,
null lettura_misura_f6 ,null delta_misure_f1 ,null delta_misure_f2 ,
null delta_misure_f3 ,null delta_misure_f4 ,null delta_misure_f5 ,
null delta_misure_f6 ,pod,CONCAT('SW_',tipo_flusso)tipo_flusso ,data_fine_fornitura_num data_lettura,
annomese_finef competenza_consumi ,'1' da_antswitch
from
(
select *
from
(SELECT cf_piva ,n_id_fornitura,competenza_consumi,pod ,tipo_misura,tipo_flusso, CONCAT(cf_piva,pod,n_id_fornitura)KK_MS,MAX(competenza_consumi) over ( partition by cf_piva,pod,n_id_fornitura) max_misura_fornitura from misure.misure_mensili_c
) MS
inner join misure.forniture_switching  forniture
ON KK_MS = KK_KEY
WHERE max_misura_fornitura <> annomese_finef
and annomese_finef <> max_annomese_fornitura
)TF where max_misura_fornitura = competenza_consumi
and CONCAT(TF.cf_piva,TF.pod,TF.n_id_fornitura,TF.annomese_finef) not in
(SELECT CONCAT(misure_non_orarie_c.cf_piva,misure_non_orarie_c.pod,misure_non_orarie_c.n_id_fornitura,misure_non_orarie_c.competenza_consumi)xx
FROM misure.misure_non_orarie_c);

DROP TABLE IF EXISTS misure.forniture_switching;

--SET hive.auto.convert.join=true;

MSCK REPAIR TABLE misure.misure_non_orarie_c;
MSCK REPAIR TABLE misure.misure_mensili_c;

DROP TABLE IF EXISTS misure.prt_tmo_mv_f_o;
DROP TABLE IF EXISTS misure.prt_tmo_mn_f_o;

DROP TABLE IF EXISTS misure.tbl_forniture;

