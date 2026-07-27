---AGGIUNGERE GHIGLIOTTINA SU INGESTIONE!!!----
---FASE DI INGESTIONE FLUSSI ORARI---un'oretta---
set Data=to_date(from_unixtime(unix_timestamp('01/11/2020','dd/MM/yyyy')))
SET hive.exec.parallel=true;

--set hive.execution.engine=spark;
--set spark.executor.memory=512m;
--set spark.serializer=org.apache.spark.serializer.KryoSerializer;

drop table  sos_202011_202012.ingestion_orari;

create table sos_202011_202012.ingestion_orari as
WITH bb AS
  ( SELECT ANNOMESE_SW,pod14,PIVA_DISTR,PIVA_UDD,TRATTAMENTO_ONLINE,t_cod_contr_disp as DP,tipo_misuratore_last  FROM sos_202011_202012.SW_CON_TRA_VIEW_RS X
   group by ANNOMESE_SW,pod14,PIVA_DISTR,PIVA_UDD,TRATTAMENTO_ONLINE,t_cod_contr_disp,tipo_misuratore_last
  ),

quarti_ext as (


SELECT a.coducquarti , a.podquarti , a.pivautentequarti , a.tipodato_e ,a.tipodato_s , a.tensione , a.trattamento_o , a.potcontrimpl , a.potdisp , a.cifreatt ,
b.progr_podsez, a.raccolta , a.validato , a.potmax , a.perdita , a.nomefile , a.annomesegiornodir , a.dataelaborazione , a.time_stamp , a.giornoquarti ,
a.annoquarti ,a.mesequarti ,a.pivadistributorequarti , a.codcontrdispquarti , a.areaquarti,
b.tipo_flusso , b.tipodato_a , b.ka , b.kr , b.kp ,
case when b.motivazione =3 then null else nvl(b.data_misura,concat(lpad(cast (a.giornoquarti as string),2,0),'/',lpad(cast (a.mesequarti as string),2,0),'/',cast (a.annoquarti as string))) end as data_misura
, b.tipo_rettifica , b.data_rilevazione , b.motivazione , b.data_prest , b.codprat_sii ,
b.gruppomis , b.forfait , b.motivazione_stima ,
a.e1 , a.e2 , a.e3 , a.e4 , a.e5 , a.e6 , a.e7 , a.e8 , a.e9 , a.e10 , a.e11 , a.e12 , a.e13 , a.e14 , a.e15 , a.e16 , a.e17 , a.e18 , a.e19 , a.e20 ,
a.e21 , a.e22 , a.e23 , a.e24 , a.e25 , a.e26 , a.e27 , a.e28 , a.e29 , a.e30 , a.e31 , a.e32 , a.e33 , a.e34 , a.e35 , a.e36 , a.e37 , a.e38 ,
a.e39 , a.e40 , a.e41 , a.e42 , a.e43 , a.e44 , a.e45 , a.e46 , a.e47 , a.e48 , a.e49 , a.e50 , a.e51 , a.e52 , a.e53 , a.e54 , a.e55 , a.e56 ,
a.e57 , a.e58 , a.e59 , a.e60 , a.e61 , a.e62 , a.e63 , a.e64 , a.e65 , a.e66 , a.e67 , a.e68 , a.e69 , a.e70 , a.e71 , a.e72 , a.e73 , a.e74 ,
a.e75 , a.e76 , a.e77 , a.e78 , a.e79 , a.e80 , a.e81 , a.e82 , a.e83 , a.e84 , a.e85 , a.e86 , a.e87 , a.e88 , a.e89 , a.e90 , a.e91 , a.e92 ,
a.e93 , a.e94 , a.e95 , a.e96 , a.e97 , a.e98 , a.e99 , a.e100 , a.er1 , a.er2 , a.er3 , a.er4 , a.er5 , a.er6 , a.er7 , a.er8 , a.er9 , a.er10 ,
a.er11 , a.er12 , a.er13 , a.er14 , a.er15 , a.er16 , a.er17 , a.er18 , a.er19 , a.er20 , a.er21 , a.er22 , a.er23 , a.er24 , a.er25 , a.er26 ,
a.er27 , a.er28 , a.er29 , a.er30 , a.er31 , a.er32 , a.er33 , a.er34 , a.er35 , a.er36 , a.er37 , a.er38 , a.er39 , a.er40 , a.er41 , a.er42 ,
a.er43 , a.er44 , a.er45 , a.er46 , a.er47 , a.er48 , a.er49 , a.er50 , a.er51 , a.er52 , a.er53 , a.er54 , a.er55 , a.er56 , a.er57 , a.er58 ,
a.er59 , a.er60 , a.er61 , a.er62 , a.er63 , a.er64 , a.er65 , a.er66 , a.er67 , a.er68 , a.er69 , a.er70 , a.er71 , a.er72 , a.er73 , a.er74 ,
a.er75 , a.er76 , a.er77 , a.er78 , a.er79 , a.er80 , a.er81 , a.er82 , a.er83 , a.er84 , a.er85 , a.er86 , a.er87 , a.er88 , a.er89 , a.er90 ,
a.er91 , a.er92 , a.er93 , a.er94 , a.er95 , a.er96 , a.er97 , a.er98 , a.er99 , a.er100,
b.data_inizio_periodo ,b.eaf1  , b.eaf2  , b.eaf3  , b.eaf4  , b.eaf5  , b.eaf6  ,
b.erf1  , b.erf2  , b.erf3  , b.erf4  , b.erf5  , b.erf6  ,b.potf1  , b.potf2  , b.potf3  , b.potf4  , b.potf5  , b.potf6 , b.eam  , b.erm  , b.potm
FROM

(select * from
AU.flusso_misure_quarti a

INNER JOIN bb
  ON SUBSTR(a.podquarti,1,14)      =bb.pod14
 where
		(

		( a.annoquarti = year(add_months(${hiveconf:Data},-1)) and a.mesequarti = month(add_months(${hiveconf:Data},-1))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-2)) and a.mesequarti = month(add_months(${hiveconf:Data},-2))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-3)) and a.mesequarti = month(add_months(${hiveconf:Data},-3))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-4)) and a.mesequarti = month(add_months(${hiveconf:Data},-4))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-5)) and a.mesequarti = month(add_months(${hiveconf:Data},-5))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-6)) and a.mesequarti = month(add_months(${hiveconf:Data},-6))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-7)) and a.mesequarti = month(add_months(${hiveconf:Data},-7))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-8)) and a.mesequarti = month(add_months(${hiveconf:Data},-8))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-9)) and a.mesequarti = month(add_months(${hiveconf:Data},-9))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-10)) and a.mesequarti = month(add_months(${hiveconf:Data},-10))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-11)) and a.mesequarti = month(add_months(${hiveconf:Data},-11))    ) OR
		( a.annoquarti = year(add_months(${hiveconf:Data},-12)) and a.mesequarti = month(add_months(${hiveconf:Data},-12))    )
		OR ( a.annoquarti = year(add_months(${hiveconf:Data},-13)) and a.mesequarti = month(add_months(${hiveconf:Data},-13))    )
		OR ( a.annoquarti = year(add_months(${hiveconf:Data},-14)) and a.mesequarti = month(add_months(${hiveconf:Data},-14))    )

		)
)

A LEFT OUTER JOIN

(select * from

au.flusso_misure_estensione_quarti b
INNER JOIN bb
  ON SUBSTR(b.podquarti,1,14)      =bb.pod14

    where
		(
		( b.annoquarti = year(add_months(${hiveconf:Data},-1)) and b.mesequarti = month(add_months(${hiveconf:Data},-1))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-2)) and b.mesequarti = month(add_months(${hiveconf:Data},-2))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-3)) and b.mesequarti = month(add_months(${hiveconf:Data},-3))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-4)) and b.mesequarti = month(add_months(${hiveconf:Data},-4))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-5)) and b.mesequarti = month(add_months(${hiveconf:Data},-5))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-6)) and b.mesequarti = month(add_months(${hiveconf:Data},-6))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-7)) and b.mesequarti = month(add_months(${hiveconf:Data},-7))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-8)) and b.mesequarti = month(add_months(${hiveconf:Data},-8))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-9)) and b.mesequarti = month(add_months(${hiveconf:Data},-9))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-10)) and b.mesequarti = month(add_months(${hiveconf:Data},-10))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-11)) and b.mesequarti = month(add_months(${hiveconf:Data},-11))    ) OR
		( b.annoquarti = year(add_months(${hiveconf:Data},-12)) and b.mesequarti = month(add_months(${hiveconf:Data},-12))    )
		OR ( b.annoquarti = year(add_months(${hiveconf:Data},-13)) and b.mesequarti = month(add_months(${hiveconf:Data},-13))    )
		OR ( b.annoquarti = year(add_months(${hiveconf:Data},-14)) and b.mesequarti = month(add_months(${hiveconf:Data},-14))    )

		)
		)
B
ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
)

SELECT a.pod14,
  a.data_misura,
  annomesegiornodir,
  a.eaf1,
  a.eaf2,
  a.eaf3,
    a.eaf4,
  a.eaf5,
  a.eaf6,
  a.eam,
  a.raccolta,trattamento,tipodato_e, tipodato_s,potf1, potf2, potf3, potf4, potf5, potf6,

   e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, e23, e24, e25, e26, e27, e28, e29, e30, e31, e32, e33, e34, e35, e36, e37, e38, e39, e40, e41, e42, e43, e44, e45, e46, e47, e48, e49, e50, e51, e52
				, e53, e54, e55, e56, e57, e58, e59, e60, e61, e62, e63, e64, e65, e66, e67, e68, e69, e70, e71, e72, e73, e74, e75, e76, e77, e78, e79, e80, e81, e82, e83, e84, e85, e86, e87, e88, e89, e90, e91, e92, e93, e94, e95, e96, e97, e98, e99, e100
				,nomefile,PIVA_DISTR,PIVA_UDD, TRATTAMENTO_ONLINE,DP,ANNOMESE_SW,tipo_misuratore_last,potmax, NULL AS TIPO_MISURATORE_MIS,motivazione, time_Stamp, anno ,mese

FROM
  (SELECT SUBSTR(b.podquarti,1,14) pod14,
    b.data_misura,
    annomesegiornodir,
    NVL(b.eaf1,0) eaf1,
    NVL(b.eaf2,0) eaf2,
    NVL(b.eaf3,0) eaf3,
    NVL(b.eaf4,0) eaf4,
    NVL(b.eaf5,0) eaf5,
    NVL(b.eaf6,0) eaf6,
    NVL(b.eam,0) eam,
    NVL(b.raccolta, 'X') raccolta ,b.TRATTAMENTO_O as trattamento,tipodato_e, tipodato_s,nvl(potf1,potm) as potf1, potf2, potf3, potf4, potf5, potf6,


     e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, e23, e24, e25, e26, e27, e28, e29, e30, e31, e32, e33, e34, e35, e36, e37, e38, e39, e40, e41, e42, e43, e44, e45, e46, e47, e48, e49, e50, e51, e52
				, e53, e54, e55, e56, e57, e58, e59, e60, e61, e62, e63, e64, e65, e66, e67, e68, e69, e70, e71, e72, e73, e74, e75, e76, e77, e78, e79, e80, e81, e82, e83, e84, e85, e86, e87, e88, e89, e90, e91, e92, e93, e94, e95, e96, e97, e98, e99, e100
				,nomefile, potmax

    ,RANK() OVER(PARTITION BY SUBSTR(podquarti,1,14), annoquarti, mesequarti, data_misura ORDER BY time_stamp DESC, annomesegiornodir DESC, dataelaborazione DESC, potmax DESC, tipo_flusso DESC) RAK,PIVA_DISTR,PIVA_UDD, bb.TRATTAMENTO_ONLINE,DP,ANNOMESE_SW,tipo_misuratore_last,motivazione, time_stamp, annoquarti as anno,mesequarti as mese
  FROM quarti_ext b
  INNER JOIN bb
  ON SUBSTR(b.podquarti,1,14)      =bb.pod14
  WHERE
--  (tipodato_e            = 1
--  OR (tipodato_e             = 0
--  AND tipodato_s             = 0))

  --AND NVL(motivazione, 'P') not in ('3','4','5')
--  and
  NVL(VALIDATO,'S')='S'
-- OKKIO COMMENTARE ANCHE SUGLI ORARI LA RACCOLTA, VEDI NUOVO DOCUMENTO LAURA		AND NVL(b.raccolta, 'X') NOT IN ('S','V') ---OKKIO OKKIO OKKIO RFO1G ora contiene la motivazione!!!

  )a
WHERE a.rak                   = 1;
