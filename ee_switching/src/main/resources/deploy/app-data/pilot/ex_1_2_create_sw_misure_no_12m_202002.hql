MAPREDUCEPARAM-FILTER

---FASE DI INGESTIONE NON ORARI E SMIS ---un'oretta ciascuna
set Data=to_date(from_unixtime(unix_timestamp(DATA-FILTER)));


drop table if exists ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.sw_misure_no_12m_202002 ;
create table ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.sw_misure_no_12m_202002 as

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
  a.raccolta,trattamento,tipodato_e, tipodato_s,nvl(potf1,potm) as potf1, potf2, potf3, potf4, potf5, potf6,

   e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, e23, e24, e25, e26, e27, e28, e29, e30, e31, e32, e33, e34, e35, e36, e37, e38, e39, e40, e41, e42, e43, e44, e45, e46, e47, e48, e49, e50, e51, e52
				, e53, e54, e55, e56, e57, e58, e59, e60, e61, e62, e63, e64, e65, e66, e67, e68, e69, e70, e71, e72, e73, e74, e75, e76, e77, e78, e79, e80, e81, e82, e83, e84, e85, e86, e87, e88, e89, e90, e91, e92, e93, e94, e95, e96, e97, e98, e99, e100
				,nomefile,PIVA_DISTR,PIVA_UDD, TRATTAMENTO_ONLINE,DP,ANNOMESE_SW,tipo_misuratore_last,potmax, NULL AS TIPO_MISURATORE_MIS,motivazione, time_stamp, anno, mese
				, d_data_decorrenza, d_creazione

FROM
  (SELECT SUBSTR(b.pod,1,14) pod14,
	case when b.motivazione =3 then null else b.data_misura end as data_misura,
    annomesegiornodir,
    NVL(b.eaf1,0) eaf1,
    NVL(b.eaf2,0) eaf2,
    NVL(b.eaf3,0) eaf3,
    NVL(b.eaf1,0) eaf4,
    NVL(b.eaf2,0) eaf5,
    NVL(b.eaf3,0) eaf6,
    NVL(b.eam,0) eam,
    NVL(b.raccolta, 'X') raccolta ,trattamento,tipodato_e, tipodato_s,potf1, potf2, potf3, potf4, potf5, potf6,


     e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, e23, e24, e25, e26, e27, e28, e29, e30, e31, e32, e33, e34, e35, e36, e37, e38, e39, e40, e41, e42, e43, e44, e45, e46, e47, e48, e49, e50, e51, e52
				, e53, e54, e55, e56, e57, e58, e59, e60, e61, e62, e63, e64, e65, e66, e67, e68, e69, e70, e71, e72, e73, e74, e75, e76, e77, e78, e79, e80, e81, e82, e83, e84, e85, e86, e87, e88, e89, e90, e91, e92, e93, e94, e95, e96, e97, e98, e99, e100
				,nomefile,potmax,potm

    ,RANK() OVER(PARTITION BY SUBSTR(pod,1,14), anno, mese, data_misura ORDER BY time_stamp DESC, annomesegiornodir DESC, dataelaborazione DESC, potmax DESC, tipo_flusso DESC) RAK,PIVA_DISTR,PIVA_UDD, TRATTAMENTO_ONLINE,DP,ANNOMESE_SW,tipo_misuratore_last,motivazione, time_Stamp, anno, mese
    , d_data_decorrenza, d_creazione
  FROM ${hiveconf:AU_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_FLUSSO_MISURE_NO_AGGR} b
  INNER JOIN ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.bb
  ON SUBSTR(b.pod,1,14)      =bb.pod14
  WHERE
  --(tipodato_e            = 1
  --OR (tipodato_e             = 0
  --AND tipodato_s             = 0))

  --AND NVL(motivazione, 'P')  not in ('3','4','5')
  --AND
  (NOMEFILE LIKE '%PNO%'
  OR NOMEFILE LIKE '%RNO%')
  and NVL(VALIDATO,'S')='S'

  and
		(( anno = year(add_months(${hiveconf:Data},-1)) and mese = month(add_months(${hiveconf:Data},-1))    ) OR
		( anno = year(add_months(${hiveconf:Data},-2)) and mese = month(add_months(${hiveconf:Data},-2))    ) OR
		( anno = year(add_months(${hiveconf:Data},-3)) and mese = month(add_months(${hiveconf:Data},-3))    ) OR
		( anno = year(add_months(${hiveconf:Data},-4)) and mese = month(add_months(${hiveconf:Data},-4))    ) OR
		( anno = year(add_months(${hiveconf:Data},-5)) and mese = month(add_months(${hiveconf:Data},-5))    ) OR
		( anno = year(add_months(${hiveconf:Data},-6)) and mese = month(add_months(${hiveconf:Data},-6))    ) OR
		( anno = year(add_months(${hiveconf:Data},-7)) and mese = month(add_months(${hiveconf:Data},-7))    ) OR
		( anno = year(add_months(${hiveconf:Data},-8)) and mese = month(add_months(${hiveconf:Data},-8))    ) OR
		( anno = year(add_months(${hiveconf:Data},-9)) and mese = month(add_months(${hiveconf:Data},-9))    ) OR
		( anno = year(add_months(${hiveconf:Data},-10)) and mese = month(add_months(${hiveconf:Data},-10))    ) OR
		( anno = year(add_months(${hiveconf:Data},-11)) and mese = month(add_months(${hiveconf:Data},-11))    ) OR
		( anno = year(add_months(${hiveconf:Data},-12)) and mese = month(add_months(${hiveconf:Data},-12))    ) OR
		( anno = year(add_months(${hiveconf:Data},-13)) and mese = month(add_months(${hiveconf:Data},-13))    )
		OR ( anno = year(add_months(${hiveconf:Data},-14)) and mese = month(add_months(${hiveconf:Data},-14))    )
		)

  )a
WHERE a.rak                   = 1
---AND NVL(a.raccolta, 'X') NOT IN ('S','V')--VA SPOSTATO DENTRO??? ---sui nuovi tracciati dobbiamo prendere anche S e V

---SMIS SMONTAGGIO E MONTAGGIO---
UNION ALL
SELECT b.pod14,
  b.data_misura,
  annomesegiornodir,
  b.eaf1,
  b.eaf2,
  b.eaf3,
  b.eaf4,
  b.eaf5,
  b.eaf6,
  b.eam,
  b.raccolta,trattamento,tipodato_e, tipodato_s,potf1, potf2, potf3, potf4, potf5, potf6,
   e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, e23, e24, e25, e26, e27, e28, e29, e30, e31, e32, e33, e34, e35, e36, e37, e38, e39, e40, e41, e42, e43, e44, e45, e46, e47, e48, e49, e50, e51, e52
				, e53, e54, e55, e56, e57, e58, e59, e60, e61, e62, e63, e64, e65, e66, e67, e68, e69, e70, e71, e72, e73, e74, e75, e76, e77, e78, e79, e80, e81, e82, e83, e84, e85, e86, e87, e88, e89, e90, e91, e92, e93, e94, e95, e96, e97, e98, e99, e100
    ,nomefile,PIVA_DISTR,PIVA_UDD, TRATTAMENTO_ONLINE,DP,ANNOMESE_SW,tipo_misuratore_last, null as potmax,TIPO_MISURATORE_MIS, null as motivazione, time_stamp, anno, mese
    , d_data_decorrenza, d_creazione



FROM
  (SELECT SUBSTR(pod,1,14) pod14,
    data_misura_smn DATA_MISURA,
    annomesegiornodir,
    NVL(eaf1_smn,0) EAF1,
    NVL(eaf2_smn,0) EAF2,
    NVL(eaf3_smn,0) EAF3,
    NVL(eaf4_smn,0) EAF4,
    NVL(eaf5_smn,0) EAF5,
    NVL(eaf6_smn,0) EAF6,
    NVL(eam_smn,0) eam,
    'SM' raccolta ,null trattamento,case when tipo_dato_smn='E' then 1 else 0 end as tipodato_e, case when tipo_dato_smn='S' then 1 else 0 end as tipodato_s,potf1_smn as potf1, potf2_smn potf2, potf3_smn potf3, potf4_smn potf4, potf5_smn potf5, potf6_smn potf6,

   null e1, null e2, null e3, null e4, null e5, null e6, null e7, null e8, null e9, null e10, null e11, null e12, null e13, null e14, null e15, null e16, null e17, null e18, null e19, null e20, null e21, null e22, null e23, null e24, null e25, null e26, null e27, null e28, null e29, null e30, null e31, null e32, null e33, null e34, null e35, null e36, null e37, null e38, null e39, null e40, null e41, null e42, null e43, null e44, null e45, null e46, null e47, null e48, null e49, null e50, null e51, null e52
				, null e53, null e54, null e55, null e56, null e57, null e58, null e59, null e60, null e61, null e62, null e63, null e64, null e65, null e66, null e67, null e68, null e69, null e70, null e71, null e72, null e73, null e74, null e75, null e76, null e77, null e78, null e79, null e80, null e81, null e82, null e83, null e84, null e85, null e86, null e87, null e88, null e89, null e90, null e91, null e92, null e93, null e94, null e95, null e96, null e97, null e98, null e99, null e100

   ,nomefile

   ,rank() over(partition BY SUBSTR(pod,1,14), data_misura_smn order by time_stamp DESC, dataelaborazione DESC, eaf1_smn DESC, eaf2_smn DESC, eaf3_smn DESC) rak,PIVA_DISTR,PIVA_UDD, TRATTAMENTO_ONLINE,DP,ANNOMESE_SW,tipo_misuratore_last, tipo_misuratore_smn AS TIPO_MISURATORE_MIS, time_Stamp,anno_dtms as anno, mese_dtms as mese
   , d_data_decorrenza, d_creazione
  FROM ${hiveconf:AU_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_FLUSSO_MISURE_SMIS} a
  INNER JOIN ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.bb
  ON SUBSTR(a.pod,1,14)=bb.pod14
    WHERE
                    			(( anno_dtms = year(add_months(${hiveconf:Data},-1)) and mese_dtms = month(add_months(${hiveconf:Data},-1))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-2)) and mese_dtms = month(add_months(${hiveconf:Data},-2))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-3)) and mese_dtms = month(add_months(${hiveconf:Data},-3))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-4)) and mese_dtms = month(add_months(${hiveconf:Data},-4))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-5)) and mese_dtms = month(add_months(${hiveconf:Data},-5))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-6)) and mese_dtms = month(add_months(${hiveconf:Data},-6))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-7)) and mese_dtms = month(add_months(${hiveconf:Data},-7))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-8)) and mese_dtms = month(add_months(${hiveconf:Data},-8))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-9)) and mese_dtms = month(add_months(${hiveconf:Data},-9))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-10)) and mese_dtms = month(add_months(${hiveconf:Data},-10))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-11)) and mese_dtms = month(add_months(${hiveconf:Data},-11))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-12)) and mese_dtms = month(add_months(${hiveconf:Data},-12))    )
									OR ( anno_dtms = year(add_months(${hiveconf:Data},-13)) and mese_dtms = month(add_months(${hiveconf:Data},-13))    )
									OR ( anno_dtms = year(add_months(${hiveconf:Data},-14)) and mese_dtms = month(add_months(${hiveconf:Data},-14))    )

                    			)
  )b
WHERE b.rak = 1
UNION ALL
SELECT c.pod14,
  c.data_misura,
  annomesegiornodir,
  c.eaf1,
  c.eaf2,
  c.eaf3,
      c.eaf4,
  c.eaf5,
  c.eaf6,
  c.eam,
  c.raccolta,trattamento,tipodato_e, tipodato_s,potf1, potf2, potf3, potf4, potf5, potf6,
   e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, e23, e24, e25, e26, e27, e28, e29, e30, e31, e32, e33, e34, e35, e36, e37, e38, e39, e40, e41, e42, e43, e44, e45, e46, e47, e48, e49, e50, e51, e52
				, e53, e54, e55, e56, e57, e58, e59, e60, e61, e62, e63, e64, e65, e66, e67, e68, e69, e70, e71, e72, e73, e74, e75, e76, e77, e78, e79, e80, e81, e82, e83, e84, e85, e86, e87, e88, e89, e90, e91, e92, e93, e94, e95, e96, e97, e98, e99, e100
    ,nomefile,PIVA_DISTR,PIVA_UDD, TRATTAMENTO_ONLINE,DP,ANNOMESE_SW,tipo_misuratore_last, null as potmax,TIPO_MISURATORE_MIS, null as motivazione, time_Stamp, anno, mese
    , d_data_decorrenza, d_creazione
FROM
  (SELECT SUBSTR(pod,1,14) pod14,
    data_misura_mn DATA_MISURA,
    annomesegiornodir,
    NVL(eaf1_mn,0) eaf1,
    NVL(eaf2_mn,0) eaf2,
    NVL(eaf3_mn,0) eaf3,
    NVL(eaf4_mn,0) eaf4,
    NVL(eaf5_mn,0) eaf5,
    NVL(eaf6_mn,0) eaf6,
    NVL(eam_mn,0) eam,
    'MN' raccolta ,null trattamento,1 tipodato_e, 0 tipodato_s,potf1_mn as potf1, potf2_mn potf2, potf3_mn potf3, potf4_mn potf4, potf5_mn potf5, potf6_mn potf6,

    null e1, null e2, null e3, null e4, null e5, null e6, null e7, null e8, null e9, null e10, null e11, null e12, null e13, null e14, null e15, null e16, null e17, null e18, null e19, null e20, null e21, null e22, null e23, null e24, null e25, null e26, null e27, null e28, null e29, null e30, null e31, null e32, null e33, null e34, null e35, null e36, null e37, null e38, null e39, null e40, null e41, null e42, null e43, null e44, null e45, null e46, null e47, null e48, null e49, null e50, null e51, null e52
				, null e53, null e54, null e55, null e56, null e57, null e58, null e59, null e60, null e61, null e62, null e63, null e64, null e65, null e66, null e67, null e68, null e69, null e70, null e71, null e72, null e73, null e74, null e75, null e76, null e77, null e78, null e79, null e80, null e81, null e82, null e83, null e84, null e85, null e86, null e87, null e88, null e89, null e90, null e91, null e92, null e93, null e94, null e95, null e96, null e97, null e98, null e99, null e100

    ,nomefile
    ,rank() over(partition BY SUBSTR(pod,1,14), data_misura_mn order by time_stamp DESC, dataelaborazione DESC, eaf1_mn ASC, eaf2_mn ASC, eaf3_mn ASC) rak,PIVA_DISTR,PIVA_UDD, TRATTAMENTO_ONLINE,DP,ANNOMESE_SW,tipo_misuratore_last, tipo_misuratore_mn AS TIPO_MISURATORE_MIS, time_Stamp,anno_dtms as anno, mese_dtms as mese
    , d_data_decorrenza, d_creazione
  FROM ${hiveconf:AU_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_FLUSSO_MISURE_SMIS} a
  INNER JOIN ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.bb
  ON SUBSTR(a.pod,1,14)=bb.pod14
  WHERE
                    			(( anno_dtms = year(add_months(${hiveconf:Data},-1)) and mese_dtms = month(add_months(${hiveconf:Data},-1))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-2)) and mese_dtms = month(add_months(${hiveconf:Data},-2))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-3)) and mese_dtms = month(add_months(${hiveconf:Data},-3))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-4)) and mese_dtms = month(add_months(${hiveconf:Data},-4))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-5)) and mese_dtms = month(add_months(${hiveconf:Data},-5))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-6)) and mese_dtms = month(add_months(${hiveconf:Data},-6))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-7)) and mese_dtms = month(add_months(${hiveconf:Data},-7))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-8)) and mese_dtms = month(add_months(${hiveconf:Data},-8))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-9)) and mese_dtms = month(add_months(${hiveconf:Data},-9))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-10)) and mese_dtms = month(add_months(${hiveconf:Data},-10))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-11)) and mese_dtms = month(add_months(${hiveconf:Data},-11))    ) OR
                        			( anno_dtms = year(add_months(${hiveconf:Data},-12)) and mese_dtms = month(add_months(${hiveconf:Data},-12))    )
									OR ( anno_dtms = year(add_months(${hiveconf:Data},-13)) and mese_dtms = month(add_months(${hiveconf:Data},-13))    )
									OR ( anno_dtms = year(add_months(${hiveconf:Data},-14)) and mese_dtms = month(add_months(${hiveconf:Data},-14))    )
                    			)



  )c
WHERE c.rak = 1;
----OCCHIO FARE L'UNION ALL DOPO-----
--union all
--select * from ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.ingestion_orari; ---flussi orari