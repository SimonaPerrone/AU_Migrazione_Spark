--creazione della vista su oracle
--SELECT u.N_ID_UDD,a.T_RAG_SOC,a.T_PIVA,u.N_ID_POD,p.T_CODICE_POD,p.T_AREA_RIF,u.D_INIZIO,u.D_FINE 
CREATE VIEW RCU.POD_POD_UDD AS 
SELECT a.T_RAG_SOC,a.T_PIVA,u.N_ID_POD,substr(p.T_CODICE_POD,1,14) POD_14,p.T_AREA_RIF,u.D_INIZIO,u.D_FINE , u.N_ID_UDD
  FROM RCU.RCU_POD p 
  JOIN RCU.RCU_POD_UDD u ON p.N_ID_POD=u.N_ID_POD
  JOIN RCU.RCU_AZIENDA a ON a.N_ID_AZIENDA=u.N_ID_UDD;
  
CREATE VIEW RCU.DISTR_AZ AS
SELECT DISTR.N_ID_DISTR, DISTR.N_ID_DISTR_RIF, DISTR.T_TIPO, AZ.T_PIVA  --pivadistributoreaggr
	FROM RCU.RCU_DISTR DISTR
	JOIN RCU.RCU_AZIENDA AZ ON DISTR.N_ID_DISTR = AZ.N_ID_AZIENDA;
  
--questa vista viene creata a runtime (attenzione alle grant su db)
CREATE VIEW RCU.IS_T_TRATTAMENTO_STATO_POD
AS SELECT p.T_CODICE_POD,tmp.STATO_POD, CASE   WHEN EXISTS (SELECT 'Y' FROM RCU.RCU_POD_MISURE a where a.n_id_pod = b.n_id_pod AND a.T_TRATTAMENTO = 'O' and  TO_DATE('201702'||01,'yyyymmdd') >= a.D_ANNO_MESE and D_ANNO_MESE = (SELECT min(D_ANNO_MESE) FROM rcu.rcu_pod_misure x where x.n_id_pod = b.n_id_pod and TO_DATE('201702'||01,'YYYYMMDD') >= x.D_ANNO_MESE)) THEN 'Y'   WHEN EXISTS (SELECT 'Y' FROM RCUS.RCUS_PODMISURE c where c.n_id_pod = b.n_id_pod  and c.T_TRATTAMENTO = 'O' and c.D_ANNO_MESE <=TRUNC(TO_DATE('201702'||01,'yyyymmdd'),'MM') and c.b_valido = 'Y' and D_ANNO_MESE = (SELECT min(D_ANNO_MESE) FROM rcus.rcus_podmisure x where x.n_id_pod = c.n_id_pod and TO_DATE('201702'||01,'YYYYMMDD') >= x.D_ANNO_MESE and x.b_valido = 'Y')) THEN 'Y' ELSE 'N' END AS IS_T_TRATTAMENTO FROM RCU.RCU_POD_MISURE b JOIN RCU.RCU_POD p ON p.n_id_pod=b.n_id_pod JOIN ( SELECT b.n_id_pod, CASE WHEN EXISTS ( 	SELECT a.n_id_pod FROM RCU.RCU_POD_STATO a 	WHERE a.n_id_pod = b.n_id_pod 	AND '201702' between TO_CHAR(NVL(a.D_ATTIVAZIONE,TO_DATE('19000101','yyyymmdd')),'YYYYMM') 	AND TO_CHAR(NVL(a.D_DISATTIVAZIONE,TO_DATE('20991231','yyyymmdd')),'YYYYMM') 	and a.D_AGGIORNAMENTO = (SELECT max(z.d_aggiornamento) from RCU.RCU_POD_STATO z where z.n_id_pod = a.n_id_pod 	and '201702' between TO_CHAR(NVL(z.D_ATTIVAZIONE,TO_DATE('19000101','yyyymmdd')),'YYYYMM') 	AND TO_CHAR(NVL(z.D_DISATTIVAZIONE,TO_DATE('20991231','yyyymmdd')),'YYYYMM')) ) THEN 'Y' WHEN EXISTS (   SELECT 'Y'  FROM RCUS.RCUS_PODSTATO r   WHERE r.n_id_pod = b.n_id_pod   and '201702'  between TO_CHAR(NVL(r.D_ATTIVAZIONE,TO_DATE('19000101','yyyymmdd')),'YYYYMM')   AND TO_CHAR(NVL(r.D_DISATTIVAZIONE,TO_DATE('20991231','yyyymmdd')),'YYYYMM') and r.b_valido='Y'   and r.D_AGGIORNAMENTO = (SELECT max(z.d_aggiornamento) from RCUS.RCUS_PODSTATO z where z.n_id_pod = r.n_id_pod   and '201702'  between TO_CHAR(NVL(r.D_ATTIVAZIONE,TO_DATE('19000101','yyyymmdd')),'YYYYMM')   AND TO_CHAR(NVL(r.D_DISATTIVAZIONE,TO_DATE('20991231','yyyymmdd')),'YYYYMM') and z.b_valido='Y') ) THEN 'Y' ELSE 'N' END AS STATO_POD FROM RCU.RCU_POD_STATO b ) tmp on tmp.n_id_pod=p.n_id_pod

  
--in esecuzione viene creata una vista che si chiama RCU.IS_T_TRATTAMENTO_STATO_POD (assicurarsi di avere le grant)

--crea lo schema au
CREATE DATABASE IF NOT EXISTS au COMMENT 'Acquirente Unico - Flusso Misure' LOCATION '/user/ec2-user/au';
--forfait String,gruppoMis String,ka DOUBLE,kr DOUBLE,eaf1 DOUBLE,eaf2 DOUBLE,eaf3 DOUBLE,eaf4 DOUBLE,eaf5 DOUBLE,eaf6 DOUBLE,erf1 DOUBLE,erf2 DOUBLE,erf3 DOUBLE,erf4 DOUBLE,erf5 DOUBLE,erf6 DOUBLE,potf1 DOUBLE,potf2 DOUBLE,potf3 DOUBLE,potf4 DOUBLE,potf5 DOUBLE,potf6 DOUBLE
--impala-shell

--indice su podquarti
--configurazioni su hive
SET hive.exec.parallel=true;
SET hive.merge.mapfiles=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.map.tasks = 3;
SET mapred.reduce.tasks = 3;
--creazione indici
DROP INDEX test_index ON au.flusso_misure_quarti;
CREATE INDEX test_index ON TABLE au.flusso_misure_quarti(podquarti) AS 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX test_index ON au.flusso_misure_quarti rebuild;

--Flusso misure quarti
CREATE EXTERNAL TABLE au.flusso_misure_quarti (
  coducquarti String, podquarti String, pivautentequarti String, 
  tipodato_e INT,tipodato_s INT,tensione DOUBLE,trattamento_o String,potcontrimpl DOUBLE,potdisp DOUBLE,cifreatt INT,cifrerea INT,raccolta String,validato String,potmax DOUBLE,perdita DOUBLE,nomefile String,annomesegiornodir INT,dataelaborazione TIMESTAMP,time_stamp BIGINT,giornoquarti INT,
  e1 DOUBLE,e2 DOUBLE,e3 DOUBLE,e4 DOUBLE,e5 DOUBLE,e6 DOUBLE,e7 DOUBLE,e8 DOUBLE,e9 DOUBLE,e10 DOUBLE,e11 DOUBLE,e12 DOUBLE,e13 DOUBLE,e14 DOUBLE,e15 DOUBLE,e16 DOUBLE,e17 DOUBLE,e18 DOUBLE,
  e19 DOUBLE,e20 DOUBLE,e21 DOUBLE,e22 DOUBLE,e23 DOUBLE,e24 DOUBLE,e25 DOUBLE,e26 DOUBLE,e27 DOUBLE,e28 DOUBLE,e29 DOUBLE,e30 DOUBLE,e31 DOUBLE,e32 DOUBLE,e33 DOUBLE,e34 DOUBLE,e35 DOUBLE,
  e36 DOUBLE,e37 DOUBLE,e38 DOUBLE,e39 DOUBLE,e40 DOUBLE,e41 DOUBLE,e42 DOUBLE,e43 DOUBLE,e44 DOUBLE,e45 DOUBLE,e46 DOUBLE,e47 DOUBLE,e48 DOUBLE,e49 DOUBLE,e50 DOUBLE,e51 DOUBLE,e52 DOUBLE,
  e53 DOUBLE,e54 DOUBLE,e55 DOUBLE,e56 DOUBLE,e57 DOUBLE,e58 DOUBLE,e59 DOUBLE,e60 DOUBLE,e61 DOUBLE,e62 DOUBLE,e63 DOUBLE,e64 DOUBLE,e65 DOUBLE,e66 DOUBLE,e67 DOUBLE,e68 DOUBLE,e69 DOUBLE,
  e70 DOUBLE,e71 DOUBLE,e72 DOUBLE,e73 DOUBLE,e74 DOUBLE,e75 DOUBLE,e76 DOUBLE,e77 DOUBLE,e78 DOUBLE,e79 DOUBLE,e80 DOUBLE,e81 DOUBLE,e82 DOUBLE,e83 DOUBLE,e84 DOUBLE,e85 DOUBLE,e86 DOUBLE,
  e87 DOUBLE,e88 DOUBLE,e89 DOUBLE,e90 DOUBLE,e91 DOUBLE,e92 DOUBLE,e93 DOUBLE,e94 DOUBLE,e95 DOUBLE,e96 DOUBLE,e97 DOUBLE,e98 DOUBLE,e99 DOUBLE,e100 DOUBLE,
  er1 DOUBLE,er2 DOUBLE,er3 DOUBLE,er4 DOUBLE,er5 DOUBLE,er6 DOUBLE,er7 DOUBLE,er8 DOUBLE,er9 DOUBLE,er10 DOUBLE,er11 DOUBLE,er12 DOUBLE,er13 DOUBLE,er14 DOUBLE,er15 DOUBLE,er16 DOUBLE,er17 DOUBLE,er18 DOUBLE,
  er19 DOUBLE,er20 DOUBLE,er21 DOUBLE,er22 DOUBLE,er23 DOUBLE,er24 DOUBLE,er25 DOUBLE,er26 DOUBLE,er27 DOUBLE,er28 DOUBLE,er29 DOUBLE,er30 DOUBLE,er31 DOUBLE,er32 DOUBLE,er33 DOUBLE,er34 DOUBLE,er35 DOUBLE,
  er36 DOUBLE,er37 DOUBLE,er38 DOUBLE,er39 DOUBLE,er40 DOUBLE,er41 DOUBLE,er42 DOUBLE,er43 DOUBLE,er44 DOUBLE,er45 DOUBLE,er46 DOUBLE,er47 DOUBLE,er48 DOUBLE,er49 DOUBLE,er50 DOUBLE,er51 DOUBLE,er52 DOUBLE,
  er53 DOUBLE,er54 DOUBLE,er55 DOUBLE,er56 DOUBLE,er57 DOUBLE,er58 DOUBLE,er59 DOUBLE,er60 DOUBLE,er61 DOUBLE,er62 DOUBLE,er63 DOUBLE,er64 DOUBLE,er65 DOUBLE,er66 DOUBLE,er67 DOUBLE,er68 DOUBLE,er69 DOUBLE,
  er70 DOUBLE,er71 DOUBLE,er72 DOUBLE,er73 DOUBLE,er74 DOUBLE,er75 DOUBLE,er76 DOUBLE,er77 DOUBLE,er78 DOUBLE,er79 DOUBLE,er80 DOUBLE,er81 DOUBLE,er82 DOUBLE,er83 DOUBLE,er84 DOUBLE,er85 DOUBLE,er86 DOUBLE,
  er87 DOUBLE,er88 DOUBLE,er89 DOUBLE,er90 DOUBLE,er91 DOUBLE,er92 DOUBLE,er93 DOUBLE,er94 DOUBLE,er95 DOUBLE,er96 DOUBLE,er97 DOUBLE,er98 DOUBLE,er99 DOUBLE,er100 DOUBLE
  
  )
  PARTITIONED BY (annoquarti INT,mesequarti INT,pivadistributorequarti String, codcontrdispquarti String, areaquarti String)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  STORED AS PARQUET
  LOCATION '/user/ec2-user/au/misure_ee_au/flusso_misure_quarti';
  
  
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY '\t'
--STORED AS PARQUET
--LOCATION '/user/ec2-user/au/misure_ee_au/max_time_stamp'
CREATE OR REPLACE VIEW au.max_time_stamp
AS 
select distinct * from (
    select pivadistributorequarti, pivautentequarti,podquarti, annoquarti,mesequarti,giornoquarti,areaquarti,validato,nomefile,codcontrdispquarti,coducquarti,
	tipodato_e,tipodato_s,tensione,trattamento_o,potcontrimpl,potdisp,cifreatt,cifrerea,raccolta,potmax,perdita,annomesegiornodir,
	 e1, e2, e3, e4, e5, e6, e7, e8, e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,e23,e24,e25,e26,e27,e28,e29,e30,e31,e32,e33,e34,e35,e36,e37,e38,e39,e40,e41,e42,e43,e44,e45,e46,e47,e48,e49,e50,
	e51,e52,e53,e54,e55,e56,e57,e58,e59,e60,e61,e62,e63,e64,e65,e66,e67,e68,e69,e70,e71,e72,e73,e74,e75,e76,e77,e78,e79,e80,e81,e82,e83,e84,e85,e86,e87,e88,e89,e90,e91,e92,e93,e94,e95,e96,e97,e98,e99,e100,
    time_stamp,max(time_stamp) over ( partition by annoquarti,mesequarti,giornoquarti,pivadistributorequarti,podquarti) time_stamp_max 
    from au.flusso_misure_quarti T 
) D where D.time_stamp_max=D.time_stamp;


CREATE EXTERNAL TABLE au.aggregazioni_misure_orarie (
  pivautente String, pod String, giorno INT, area String,validato String,nomefile String, codcontrdisp String,
  coduc String,tipodato_e INT,tipodato_s INT,tensione DOUBLE,trattamento_o String,potcontrimpl DOUBLE,potdisp DOUBLE,cifreatt INT,cifrerea INT,raccolta String,potmax DOUBLE,perdita DOUBLE,annomesegiornodir INT,
  h1 DOUBLE,h2 DOUBLE,h3 DOUBLE,h4 DOUBLE,h5 DOUBLE,h6 DOUBLE,h7 DOUBLE,h8 DOUBLE,h9 DOUBLE,h10 DOUBLE,h11 DOUBLE,h12 DOUBLE,h13 DOUBLE,h14 DOUBLE,h15 DOUBLE,h16 DOUBLE,h17 DOUBLE,h18 DOUBLE,h19 DOUBLE,h20 DOUBLE,h21 DOUBLE,h22 DOUBLE,h23 DOUBLE,h24 DOUBLE,h25 DOUBLE,
  time_stamp BIGINT,dataelaborazione TIMESTAMP,flaguddpod String, stato String, trattamento String, flagarea String, n_id_udd String, t_piva String,n_id_distr String, n_id_distr_rif String, flag_validazione String
  )
  PARTITIONED BY (anno INT, mese INT, pivadistributore String,versione BIGINT)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  STORED AS PARQUET
  LOCATION '/user/ec2-user/au/misure_ee_au/aggregazioni_misure_orarie';
  
--ALTER TABLE au.aggregazioni_misure_orarie ADD PARTITION(anno=2018,mese=1,pivadistributore='init',versione=0);

--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY '\t'
--STORED AS PARQUET
--LOCATION '/user/ec2-user/au/misure_ee_au/aggreagati_am_view'
--max sul dataelaborazione
CREATE OR REPLACE VIEW au.aggreagati_am_view
AS 
select n_id_udd,n_id_distr,n_id_distr_rif,area,giorno as giornoaggr,dataelaborazione, 
SUM(h1) h1,SUM(h2) h2,SUM(h3) h3,SUM(h4) h4,SUM(h5) h5,SUM(h6) h6,SUM(h7) h7,SUM(h8) h8,SUM(h9) h9,SUM(h10) h10,SUM(h11) h11,SUM(h12) h12,SUM(h13) h13,
SUM(h14) h14,SUM(h15) h15,SUM(h16) h16,SUM(h17) h17,SUM(h18) h18,SUM(h19) h19,SUM(h20) h20,SUM(h21) h21,SUM(h22) h22,SUM(h23) h23,SUM(h24) h24,SUM(h25) h25,
anno as annoaggr,mese as meseaggr,pivadistributore as pivadistributoreaggr,versione versione_orarie
from (
    select n_id_udd,area,pivadistributore,anno,mese,giorno,dataelaborazione,n_id_distr,n_id_distr_rif,flag_validazione,
    h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11,h12,h13,h14,h15,h16,h17,h18,h19,h20,h21,h22,h23,h24,h25,
    max(versione) over ( partition by n_id_udd,n_id_distr,anno,mese,giorno,area) versione_1, versione
    from au.aggregazioni_misure_orarie where flag_validazione='Y'
) T
where versione=versione_1
GROUP BY n_id_udd,area,pivadistributore,anno,mese,giorno,dataelaborazione,n_id_distr,n_id_distr_rif,flag_validazione,versione;

  
CREATE EXTERNAL TABLE au.aggregazioni_misure_am (
  n_id_udd String,n_id_distr String, n_id_distr_rif String, area String, giornoaggr INT, dataelaborazione TIMESTAMP,versione_orarie BIGINT,
  h1 DOUBLE,h2 DOUBLE,h3 DOUBLE,h4 DOUBLE,h5 DOUBLE,h6 DOUBLE,h7 DOUBLE,h8 DOUBLE,h9 DOUBLE,h10 DOUBLE,h11 DOUBLE,h12 DOUBLE,h13 DOUBLE,h14 DOUBLE,h15 DOUBLE,h16 DOUBLE,h17 DOUBLE,h18 DOUBLE,h19 DOUBLE,h20 DOUBLE,h21 DOUBLE,h22 DOUBLE,h23 DOUBLE,h24 DOUBLE,h25 DOUBLE
  )
  PARTITIONED BY (annoaggr INT,meseaggr INT,pivadistributoreaggr String,versione BIGINT)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  STORED AS PARQUET
  LOCATION '/user/ec2-user/au/misure_ee_au/aggregazioni_misure_am';
  

CREATE EXTERNAL TABLE au.report_ingestione (
  codice String, nomefile String, messaggio String, dataelaborazione TIMESTAMP
  )
  PARTITIONED BY (annomese INT)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  STORED AS PARQUET
  LOCATION '/user/ec2-user/au/misure_ee_au/report_ingestione'; 
  
  

  INVALIDATE METADATA;
  REFRESH flusso_misure_raggruppate;

  --hdfs://localhost.localdomain:8020/misure_ee_au/flusso_misure_raggruppate

  --In Impala, initially the partitions and data are not visible.
  --Running ALTER TABLE with the RECOVER PARTITIONS clause scans the table data directory
  --to find any new partition directories, and the data files inside them
  --ALTER TABLE flusso_misure_raggruppate RECOVER PARTITIONS;
  --sqlCtx.sql("INVALIDATE METADATA")
  --sqlCtx.sql("ALTER TABLE flusso_misure_raggruppate RECOVER PARTITIONS")

  --
  set exec.dynamic.partition=true;                                                                           
  set exec.dynamic.partition.mode=nonstrict;
  
  --utilità
  ALTER TABLE au.aggregazioni_misure_orarie SET TBLPROPERTIES('EXTERNAL'='FALSE');
  ALTER TABLE au.aggregazioni_misure_orarie DROP PARTITION (pivadistributore='init');
  
  --aggiorna le partizioni su hive
  MSCK REPAIR TABLE au.flusso_misure_quarti;
  MSCK REPAIR TABLE au.aggregazioni_misure_orarie;
  MSCK REPAIR TABLE au.aggregazioni_misure_am;
  MSCK REPAIR TABLE au.aggregazioni_misure_ip;
  MSCK REPAIR TABLE au.report_ingestione;

  --aggiorna le partizioni su impala
  ALTER TABLE au.flusso_misure_quarti RECOVER PARTITIONS;
  ALTER TABLE au.aggregazioni_misure_orarie RECOVER PARTITIONS;
  ALTER TABLE au.aggregazioni_misure_am RECOVER PARTITIONS;
  ALTER TABLE au.aggregazioni_misure_ip RECOVER PARTITIONS;
  ALTER TABLE au.report_ingestione RECOVER PARTITIONS;

  SELECT * FROM au.flusso_misure_quarti limit 10;
  SELECT * FROM au.aggregazioni_misure_orarie limit 10;
  SELECT * FROM au.aggregazioni_misure_am limit 10;
  SELECT * FROM au.aggregazioni_misure_ip limit 10;
  SELECT * FROM au.report_ingestione limit 10;
  
  --query su tabella aggregati oracle
  -- DISTINCT per prendere una volta ogni record se si lancia più volte aggregazione am
  -- MAX(b.UID_ELAB) per prendere l'ultima aggregazione oraria
  SELECT DISTINCT * FROM PRT_TMO_AGGREGATI_CALCOLATI a WHERE a.UID_ELAB=(SELECT MAX(b.UID_ELAB) FROM PRT_TMO_AGGREGATI_CALCOLATI b GROUP BY a.N_ID_UDD)
  --and a.giorno=1;
  SELECT DISTINCT N_ID_UDD,N_ID_DISTR,T_AREA_RIF,ANNOMESE,GIORNO,D_DATA_AGGREGAZIONE,UID_ELAB,
  N_H1,N_H2,N_H3,N_H4,N_H5,N_H6,N_H7,N_H8,N_H9,N_H10,N_H11,N_H12,N_H13,N_H14,N_H15,N_H16,N_H17,N_H18,N_H19,N_H20,N_H21,N_H22,N_H23,N_H24,N_H25
  FROM PRT_TMO_AGGREGATI_CALCOLATI a WHERE a.UID_ELAB=(SELECT MAX(b.UID_ELAB) FROM PRT_TMO_AGGREGATI_CALCOLATI b WHERE b.N_ID_UDD=a.N_ID_UDD);
  --and a.giorno=1;

  DROP TABLE au.flusso_misure_quarti;
  DROP TABLE au.aggregazioni_misure_orarie;
  DROP TABLE au.aggregazioni_misure_am;
  DROP TABLE au.report_ingestione;
  DROP VIEW au.max_time_stamp;
  DROP VIEW au.aggreagati_am_view;
  
  DROP TABLE au.max_time_stamp;
  DROP TABLE au.aggreagati_am_view;
  
  --quando non riesce a droppare una tabella Error while processing statement: FAILED: 
  --Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Invalid partition key & values; keys 
  --[annoquarti, mesequarti, dataelaborazione, pivadistributorequarti, ], values [2017, ])
  ALTER TABLE default.flusso_misure_quarti DROP IF EXISTS PARTITION (annoquarti=2017); //la partizione riportata nel messaggio d''errore


  --pyspark
  df = sqlContext.read.parquet("/user/silvia/misure_ee_au/flusso_misure_raggruppate")
  df.select('pod','pivautente','data_misurazione','h1','h2').show()

  --spark-shell
  val df = sqlContext.read.parquet("/user/silvia/misure_ee_au/flusso_misure_raggruppate")

  --shell
  hdfs dfs -mkdir /misure_ee_au
  hdfs dfs -put Misure_EE_AU/star.. /user/silvia/misure_ee_au/flusso_misure/

  spark-submit --class it.au.misure.GuidubaldoProj flusso-misure-0.0.5-SNAPSHOT.jar
  spark-submit --master yarn-client --class it.au.misure.GuidubaldoProj flusso-misure-0.0.20-SNAPSHOT.jar file:///home/training/Desktop/au_test_20170807/flusso_misure/______asterisco_____/2017 hdfs://elephant/misure_ee_au/flusso_misure_orarie hdfs://elephant/misure_ee_au/flusso_misure_quarti hdfs://elephant/misure_ee_au/flusso_misure_aggregate


--vecchia versione
   CREATE EXTERNAL TABLE aggregazioni_misure_orarie (
  codcontrdisp String, coduc String, pod String, pivautente String, area String, giorno INT,
  data_misurazione TIMESTAMP, stato String,  trattamento String, flaguddpod String, flagarea String,
  tipodato_e INT,tipodato_s INT,tensione DOUBLE,trattamento_o String,potcontrimpl DOUBLE,potdisp DOUBLE,cifreatt INT,cifrerea INT,raccolta String,validato String,potmax DOUBLE,nomefile String,
  h1 DOUBLE,h2 DOUBLE,h3 DOUBLE,h4 DOUBLE,h5 DOUBLE,h6 DOUBLE,h7 DOUBLE,h8 DOUBLE,h9 DOUBLE,h10 DOUBLE,h11 DOUBLE,h12 DOUBLE,h13 DOUBLE,h14 DOUBLE,h15 DOUBLE,h16 DOUBLE,h17 DOUBLE,h18 DOUBLE,h19 DOUBLE,h20 DOUBLE,h21 DOUBLE,h22 DOUBLE,h23 DOUBLE,h24 DOUBLE
  )
  PARTITIONED BY (anno INT, mese INT, dataelaborazione BIGINT, pivadistributore STRING)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  STORED AS PARQUET
  LOCATION '/user/ec2-user/au/misure_ee_au/aggregazioni_misure_orarie';
  
SELECT * FROM au.aggregazioni_misure_orarie 
WHERE flaguddpod ='N' OR flaguddpod ='E'
OR n_id_udd='N' OR n_id_udd='E'
OR t_piva='N' OR t_piva='E' OR t_piva='SK'
OR stato='SK' OR stato='E'
OR trattamento='SK' OR trattamento='E'
OR flagarea='N' OR flagarea='SK' OR flagarea='E';
