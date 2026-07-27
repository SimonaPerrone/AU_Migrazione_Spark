MAPREDUCEPARAM-FILTER

---FASE DI CREAZIONE DELLA TABELLA CON con dati per fasce e quartorarie---20 minuti
-----#OK QUESTA# versione con divisione sui giorni come da formula indicata

set Data=to_date(from_unixtime(unix_timestamp(DATA-FILTER)));



DROP TABLE IF EXISTS ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.TEST_XML_RICONF;


CREATE TABLE ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.TEST_XML_RICONF AS WITH fasce_Riconfig AS
  ( SELECT annomese_sw,
           pod14 ,
           sum(eaf1_riconf) AS somma_eaf1_riconf ,
           sum(eaf2_riconf) AS somma_eaf2_riconf ,
           sum(eaf3_riconf) AS somma_eaf3_riconf ,
           count(data_misura) AS giorni_RICONF ---non bisogna dividere
 ,
           anno_misura_riconf,
           mese_misura_riconf,
           d_data_decorrenza,
           d_creazione
   FROM ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.test_giorni_somma
   WHERE ( (anno_misura_riconf = year(add_months(${hiveconf:Data},-1))
            AND mese_misura_riconf = month(add_months(${hiveconf:Data},-1)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-2))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-2)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-3))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-3)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-4))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-4)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-5))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-5)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-6))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-6)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-7))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-7)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-8))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-8)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-9))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-9)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-10))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-10)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-11))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-11)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-12))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-12)))
          OR (anno_misura_riconf = year(add_months(${hiveconf:Data},-13))
              AND mese_misura_riconf = month(add_months(${hiveconf:Data},-13))) ) --group by annomese_sw,MESSA_REGIME,TIPO_MISURATORE,nome_flusso,pod14,piva_distr,piva_udd,trattamento,trattamento_online,dp,case when last_tipodato_s = 0 then 'E' else 'S' end,last_potf1,last_potf2,last_potf3,last_potf4,last_potf5,last_potf6,last_potmax, ---VERSIONE OLD EFFETTIVO STIMATO

   GROUP BY annomese_sw,
            pod14,
            anno_misura_riconf,
            mese_misura_riconf,
            d_data_decorrenza,
            d_creazione ),
             somma_fasce AS
  (SELECT annomese_sw,
          MESSA_REGIME,
          TIPO_MISURATORE,
          nome_flusso,
          pod14,
          piva_distr,
          piva_udd,
          trattamento,
          trattamento_online,
          dp, --case when last_tipodato_s = 0 then 'E' else 'S' end tipo_dato, ----VERSIONE OLD EFFETTIVO STIMATO
 CASE
     WHEN last_tipodato_E = 0 THEN 'S'
     ELSE last_tipodato_E
 END tipo_dato,
 CAST (last_potf1 AS string) AS last_potf1,
      CAST (last_potf2 AS string) AS last_potf2,
           CAST (last_potf3 AS string) AS last_potf3,
                CAST (last_potf4 AS string) AS last_potf4,
                     CAST (last_potf5 AS string) AS last_potf5,
                          CAST (last_potf6 AS string) AS last_potf6,
                               CAST (last_potmax AS string) AS last_potmax ,
                                    sum(delta_eaf1) AS somma_eaf1 ,
                                    sum(delta_eaf2) AS somma_eaf2 ,
                                    sum(delta_eaf3) AS somma_eaf3 ,
                                    sum(delta_eaf4) AS somma_eaf4 ,
                                    sum(delta_eaf5) AS somma_eaf5 ,
                                    sum(delta_eaf6) AS somma_eaf6 ,
                                    sum(delta_eam) AS somma_eam ,
                                    sum(giorni)somma_giorni ,
                                    datediff(last_day(to_date(from_unixtime(unix_timestamp(concat('01','/',mese_misura_next,'/',anno_misura_next),'dd/MM/yyyy')))), to_date(from_unixtime(unix_timestamp(concat('01','/',mese_misura_next,'/',anno_misura_next),'dd/MM/yyyy'))))+1 AS giorni_mese ,
                                    anno_misura_next,
                                    mese_misura_next,
                                    d_data_decorrenza,
                                    d_creazione
   FROM ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.test_giorni_somma
   WHERE ( (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-1))
            AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-1)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-2))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-2)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-3))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-3)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-4))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-4)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-5))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-5)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-6))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-6)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-7))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-7)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-8))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-8)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-9))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-9)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-10))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-10)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-11))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-11)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-12))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-12)))
          OR (ANNO_MISURA_NEXT = year(add_months(${hiveconf:Data},-13))
              AND MESE_MISURA_NEXT = month(add_months(${hiveconf:Data},-13))) ) --group by annomese_sw,MESSA_REGIME,TIPO_MISURATORE,nome_flusso,pod14,piva_distr,piva_udd,trattamento,trattamento_online,dp,case when last_tipodato_s = 0 then 'E' else 'S' end,last_potf1,last_potf2,last_potf3,last_potf4,last_potf5,last_potf6,last_potmax, ---VERSIONE OLD EFFETTIVO STIMATO

   GROUP BY annomese_sw,
            MESSA_REGIME,
            TIPO_MISURATORE,
            nome_flusso,
            pod14,
            piva_distr,
            piva_udd,
            trattamento,
            trattamento_online,
            dp,
            CASE
                WHEN last_tipodato_E = 0 THEN 'S'
                ELSE last_tipodato_E
            END,
            last_potf1,
            last_potf2,
            last_potf3,
            last_potf4,
            last_potf5,
            last_potf6,
            last_potmax,
            anno_misura_next,
            mese_misura_next,
            d_data_decorrenza,
            d_creazione) ,
             dati_curva AS
  ( SELECT DISTINCT annomese_sw,
                    pod14,
                    data_misura, --e' stato aggiunto un distinct
 CAST (e1 AS string) AS e1,
      CAST (e2 AS string) AS e2,
           CAST (e3 AS string) AS e3,
                CAST (e4 AS string) AS e4,
                     CAST (e5 AS string) AS e5,
                          CAST (e6 AS string) AS e6,
                               CAST (e7 AS string) AS e7,
                                    CAST (e8 AS string) AS e8,
                                         CAST (e9 AS string) AS e9,
                                              CAST (e10 AS string) AS e10,
                                                   CAST (e11 AS string) AS e11,
                                                        CAST (e12 AS string) AS e12,
                                                             CAST (e13 AS string) AS e13,
                                                                  CAST (e14 AS string) AS e14,
                                                                       CAST (e15 AS string) AS e15,
                                                                            CAST (e16 AS string) AS e16,
                                                                                 CAST (e17 AS string) AS e17,
                                                                                      CAST (e18 AS string) AS e18,
                                                                                           CAST (e19 AS string) AS e19,
                                                                                                CAST (e20 AS string) AS e20,
                                                                                                     CAST (e21 AS string) AS e21,
                                                                                                          CAST (e22 AS string) AS e22,
                                                                                                               CAST (e23 AS string) AS e23,
                                                                                                                    CAST (e24 AS string) AS e24,
                                                                                                                         CAST (e25 AS string) AS e25,
                                                                                                                              CAST (e26 AS string) AS e26,
                                                                                                                                   CAST (e27 AS string) AS e27,
                                                                                                                                        CAST (e28 AS string) AS e28,
                                                                                                                                             CAST (e29 AS string) AS e29,
                                                                                                                                                  CAST (e30 AS string) AS e30,
                                                                                                                                                       CAST (e31 AS string) AS e31,
                                                                                                                                                            CAST (e32 AS string) AS e32,
                                                                                                                                                                 CAST (e33 AS string) AS e33,
                                                                                                                                                                      CAST (e34 AS string) AS e34,
                                                                                                                                                                           CAST (e35 AS string) AS e35,
                                                                                                                                                                                CAST (e36 AS string) AS e36,
                                                                                                                                                                                     CAST (e37 AS string) AS e37,
                                                                                                                                                                                          CAST (e38 AS string) AS e38,
                                                                                                                                                                                               CAST (e39 AS string) AS e39,
                                                                                                                                                                                                    CAST (e40 AS string) AS e40,
                                                                                                                                                                                                         CAST (e41 AS string) AS e41,
                                                                                                                                                                                                              CAST (e42 AS string) AS e42,
                                                                                                                                                                                                                   CAST (e43 AS string) AS e43,
                                                                                                                                                                                                                        CAST (e44 AS string) AS e44,
                                                                                                                                                                                                                             CAST (e45 AS string) AS e45,
                                                                                                                                                                                                                                  CAST (e46 AS string) AS e46,
                                                                                                                                                                                                                                       CAST (e47 AS string) AS e47,
                                                                                                                                                                                                                                            CAST (e48 AS string) AS e48,
                                                                                                                                                                                                                                                 CAST (e49 AS string) AS e49,
                                                                                                                                                                                                                                                      CAST (e50 AS string) AS e50,
                                                                                                                                                                                                                                                           CAST (e51 AS string) AS e51,
                                                                                                                                                                                                                                                                CAST (e52 AS string) AS e52,
                                                                                                                                                                                                                                                                     CAST (e53 AS string) AS e53,
                                                                                                                                                                                                                                                                          CAST (e54 AS string) AS e54,
                                                                                                                                                                                                                                                                               CAST (e55 AS string) AS e55,
                                                                                                                                                                                                                                                                                    CAST (e56 AS string) AS e56,
                                                                                                                                                                                                                                                                                         CAST (e57 AS string) AS e57,
                                                                                                                                                                                                                                                                                              CAST (e58 AS string) AS e58,
                                                                                                                                                                                                                                                                                                   CAST (e59 AS string) AS e59,
                                                                                                                                                                                                                                                                                                        CAST (e60 AS string) AS e60,
                                                                                                                                                                                                                                                                                                             CAST (e61 AS string) AS e61,
                                                                                                                                                                                                                                                                                                                  CAST (e62 AS string) AS e62,
                                                                                                                                                                                                                                                                                                                       CAST (e63 AS string) AS e63,
                                                                                                                                                                                                                                                                                                                            CAST (e64 AS string) AS e64,
                                                                                                                                                                                                                                                                                                                                 CAST (e65 AS string) AS e65,
                                                                                                                                                                                                                                                                                                                                      CAST (e66 AS string) AS e66,
                                                                                                                                                                                                                                                                                                                                           CAST (e67 AS string) AS e67,
                                                                                                                                                                                                                                                                                                                                                CAST (e68 AS string) AS e68,
                                                                                                                                                                                                                                                                                                                                                     CAST (e69 AS string) AS e69,
                                                                                                                                                                                                                                                                                                                                                          CAST (e70 AS string) AS e70,
                                                                                                                                                                                                                                                                                                                                                               CAST (e71 AS string) AS e71,
                                                                                                                                                                                                                                                                                                                                                                    CAST (e72 AS string) AS e72,
                                                                                                                                                                                                                                                                                                                                                                         CAST (e73 AS string) AS e73,
                                                                                                                                                                                                                                                                                                                                                                              CAST (e74 AS string) AS e74,
                                                                                                                                                                                                                                                                                                                                                                                   CAST (e75 AS string) AS e75,
                                                                                                                                                                                                                                                                                                                                                                                        CAST (e76 AS string) AS e76,
                                                                                                                                                                                                                                                                                                                                                                                             CAST (e77 AS string) AS e77,
                                                                                                                                                                                                                                                                                                                                                                                                  CAST (e78 AS string) AS e78,
                                                                                                                                                                                                                                                                                                                                                                                                       CAST (e79 AS string) AS e79,
                                                                                                                                                                                                                                                                                                                                                                                                            CAST (e80 AS string) AS e80,
                                                                                                                                                                                                                                                                                                                                                                                                                 CAST (e81 AS string) AS e81,
                                                                                                                                                                                                                                                                                                                                                                                                                      CAST (e82 AS string) AS e82,
                                                                                                                                                                                                                                                                                                                                                                                                                           CAST (e83 AS string) AS e83,
                                                                                                                                                                                                                                                                                                                                                                                                                                CAST (e84 AS string) AS e84,
                                                                                                                                                                                                                                                                                                                                                                                                                                     CAST (e85 AS string) AS e85,
                                                                                                                                                                                                                                                                                                                                                                                                                                          CAST (e86 AS string) AS e86,
                                                                                                                                                                                                                                                                                                                                                                                                                                               CAST (e87 AS string) AS e87,
                                                                                                                                                                                                                                                                                                                                                                                                                                                    CAST (e88 AS string) AS e88,
                                                                                                                                                                                                                                                                                                                                                                                                                                                         CAST (e89 AS string) AS e89,
                                                                                                                                                                                                                                                                                                                                                                                                                                                              CAST (e90 AS string) AS e90,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                   CAST (e91 AS string) AS e91,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                        CAST (e92 AS string) AS e92,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                             CAST (e93 AS string) AS e93,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CAST (e94 AS string) AS e94,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       CAST (e95 AS string) AS e95,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            CAST (e96 AS string) AS e96,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 CAST (e97 AS string) AS e97,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      CAST (e98 AS string) AS e98,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           CAST (e99 AS string) AS e99,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                CAST (e100 AS string) AS e100 ,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     anno_misura,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     mese_misura,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     d_data_decorrenza,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     d_creazione
   FROM ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.test_giorni_somma
   WHERE ((ANNO_MISURA = year(add_months(${hiveconf:Data},-1))
           AND MESE_MISURA = month(add_months(${hiveconf:Data},-1)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-2))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-2)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-3))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-3)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-4))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-4)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-5))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-5)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-6))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-6)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-7))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-7)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-8))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-8)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-9))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-9)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-10))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-10)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-11))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-11)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-12))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-12)))
          OR (ANNO_MISURA = year(add_months(${hiveconf:Data},-13))
              AND MESE_MISURA = month(add_months(${hiveconf:Data},-13))) ) )
SELECT x.annomese_sw,
       nome_flusso,
       x.pod14,
       piva_distr,
       piva_udd,
       trattamento,
       trattamento_online,
       dp,
       tipo_dato,
       last_potf1,
       last_potf2,
       last_potf3,
       last_potf4,
       last_potf5,
       last_potf6,
       last_potmax,
       CAST (round((somma_eaf1/somma_giorni)*giorni_mese,3)AS string) AS somma_eaf1,
            CAST (round((somma_eaf2/somma_giorni)*giorni_mese,3)AS string) AS somma_eaf2,
                 CAST (round((somma_eaf3/somma_giorni)*giorni_mese,3)AS string) AS somma_eaf3,
                      CAST (round((somma_eaf4/somma_giorni)*giorni_mese,3)AS string) AS somma_eaf4,
                           CAST (round((somma_eaf5/somma_giorni)*giorni_mese,3)AS string) AS somma_eaf5,
                                CAST (round((somma_eaf6/somma_giorni)*giorni_mese,3)AS string) AS somma_eaf6,
                                     CAST (round((somma_eam/somma_giorni)*giorni_mese,3)AS string) AS somma_eam,
                                          somma_giorni,
                                          giorni_mese,
                                          anno_misura_next,
                                          mese_misura_next,
                                          y.pod14 pod,
                                          data_misura,
                                          e1,
                                          e2,
                                          e3,
                                          e4,
                                          e5,
                                          e6,
                                          e7,
                                          e8,
                                          e9,
                                          e10,
                                          e11,
                                          e12,
                                          e13,
                                          e14,
                                          e15,
                                          e16,
                                          e17,
                                          e18,
                                          e19,
                                          e20,
                                          e21,
                                          e22,
                                          e23,
                                          e24,
                                          e25,
                                          e26,
                                          e27,
                                          e28,
                                          e29,
                                          e30,
                                          e31,
                                          e32,
                                          e33,
                                          e34,
                                          e35,
                                          e36,
                                          e37,
                                          e38,
                                          e39,
                                          e40,
                                          e41,
                                          e42,
                                          e43,
                                          e44,
                                          e45,
                                          e46,
                                          e47,
                                          e48,
                                          e49,
                                          e50,
                                          e51,
                                          e52,
                                          e53,
                                          e54,
                                          e55,
                                          e56,
                                          e57,
                                          e58,
                                          e59,
                                          e60,
                                          e61,
                                          e62,
                                          e63,
                                          e64,
                                          e65,
                                          e66,
                                          e67,
                                          e68,
                                          e69,
                                          e70,
                                          e71,
                                          e72,
                                          e73,
                                          e74,
                                          e75,
                                          e76,
                                          e77,
                                          e78,
                                          e79,
                                          e80,
                                          e81,
                                          e82,
                                          e83,
                                          e84,
                                          e85,
                                          e86,
                                          e87,
                                          e88,
                                          e89,
                                          e90,
                                          e91,
                                          e92,
                                          e93,
                                          e94,
                                          e95,
                                          e96,
                                          e97,
                                          e98,
                                          e99,
                                          e100,
                                          anno_misura,
                                          mese_misura,
                                          MESSA_REGIME,
                                          TIPO_MISURATORE,
                                          CAST (round((somma_eaf1_riconf/giorni_riconf)*giorni_mese,3)AS string) AS somma_eaf1_riconf,
                                               CAST (round((somma_eaf2_riconf/giorni_riconf)*giorni_mese,3)AS string) AS somma_eaf2_riconf,
                                                    CAST (round((somma_eaf3_riconf/giorni_riconf)*giorni_mese,3)AS string) AS somma_eaf3_riconf,
                                          x.d_data_decorrenza,
                                          x.d_creazione
FROM somma_fasce x
INNER JOIN dati_curva y ON x.pod14=y.pod14
AND x.anno_misura_next=y.anno_misura
AND x.mese_misura_next=y.mese_misura
AND x.annomese_sw=y.annomese_sw
AND x.d_data_decorrenza=y.d_data_decorrenza
AND x.d_creazione=y.d_creazione
INNER JOIN fasce_Riconfig gg ON x.pod14=gg.pod14
AND gg.anno_misura_riconf=y.anno_misura
AND gg.mese_misura_riconf=y.mese_misura
AND x.annomese_sw=gg.annomese_sw
AND gg.d_data_decorrenza=y.d_data_decorrenza
AND gg.d_creazione=y.d_creazione;
--ORDER BY data_misura ASC ;
