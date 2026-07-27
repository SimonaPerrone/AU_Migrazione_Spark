SET hive.exec.dynamic.partition=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers=NONE;

CREATE
TEMPORARY TABLE ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_STORICI_RICONF_TABLE_NAME_PURGED} AS
SELECT `(d_creazione_next)?+.+`
FROM
  (SELECT *,
          lead(d_creazione) over (partition BY pod_config, d_data_decorrenza
                                  ORDER BY d_creazione ASC) AS d_creazione_next
   FROM ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_STORICI_RICONF_TABLE_NAME}
   where( 1=1 and PODRICONF-FILTER and ANNOMESE-FILTER and SINGLE-DATA-DECORRENZA-FILTER))t1
WHERE t1.d_creazione_next IS NULL;

INSERT INTO TABLE ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_STORICI_OUTPUT_TABLE_NAME} PARTITION (annomese_sw,nome_flusso)
SELECT  testxml.d_data_decorrenza
        , testxml.pod14
        , testxml.piva_distr
        , testxml.piva_udd
        , testxml.trattamento
        , testxml.trattamento_online
        , testxml.dp
        , testxml.tipo_dato
        , testxml.last_potf1
        , testxml.last_potf2
        , testxml.last_potf3
        , testxml.last_potf4
        , testxml.last_potf5
        , testxml.last_potf6
        , testxml.last_potmax
        , testxml.somma_eaf1
        , testxml.somma_eaf2
        , testxml.somma_eaf3
        , testxml.somma_eaf4
        , testxml.somma_eaf5
        , testxml.somma_eaf6
        , testxml.somma_eam
        , testxml.somma_giorni
        , testxml.giorni_mese
        , testxml.anno_misura_next
        , testxml.mese_misura_next
        , testxml.pod
        , testxml.data_misura
        , testxml.e1
        , testxml.e2
        , testxml.e3
        , testxml.e4
        , testxml.e5
        , testxml.e6
        , testxml.e7
        , testxml.e8
        , testxml.e9
        , testxml.e10
        , testxml.e11
        , testxml.e12
        , testxml.e13
        , testxml.e14
        , testxml.e15
        , testxml.e16
        , testxml.e17
        , testxml.e18
        , testxml.e19
        , testxml.e20
        , testxml.e21
        , testxml.e22
        , testxml.e23
        , testxml.e24
        , testxml.e25
        , testxml.e26
        , testxml.e27
        , testxml.e28
        , testxml.e29
        , testxml.e30
        , testxml.e31
        , testxml.e32
        , testxml.e33
        , testxml.e34
        , testxml.e35
        , testxml.e36
        , testxml.e37
        , testxml.e38
        , testxml.e39
        , testxml.e40
        , testxml.e41
        , testxml.e42
        , testxml.e43
        , testxml.e44
        , testxml.e45
        , testxml.e46
        , testxml.e47
        , testxml.e48
        , testxml.e49
        , testxml.e50
        , testxml.e51
        , testxml.e52
        , testxml.e53
        , testxml.e54
        , testxml.e55
        , testxml.e56
        , testxml.e57
        , testxml.e58
        , testxml.e59
        , testxml.e60
        , testxml.e61
        , testxml.e62
        , testxml.e63
        , testxml.e64
        , testxml.e65
        , testxml.e66
        , testxml.e67
        , testxml.e68
        , testxml.e69
        , testxml.e70
        , testxml.e71
        , testxml.e72
        , testxml.e73
        , testxml.e74
        , testxml.e75
        , testxml.e76
        , testxml.e77
        , testxml.e78
        , testxml.e79
        , testxml.e80
        , testxml.e81
        , testxml.e82
        , testxml.e83
        , testxml.e84
        , testxml.e85
        , testxml.e86
        , testxml.e87
        , testxml.e88
        , testxml.e89
        , testxml.e90
        , testxml.e91
        , testxml.e92
        , testxml.e93
        , testxml.e94
        , testxml.e95
        , testxml.e96
        , testxml.e97
        , testxml.e98
        , testxml.e99
        , testxml.e100
        , testxml.anno_misura
        , testxml.mese_misura
        , testxml.messa_regime
        , testxml.tipo_misuratore
        , 'NO' as t_tipo_configurazione
        , NULL as somma_eaf1_riconf
        , NULL as somma_eaf2_riconf
        , NULL as somma_eaf3_riconf
        , testxml.d_creazione
        , from_unixtime(unix_timestamp('${hiveconf:loading_timestamp}','yyyy-MM-dd HH:mm:ss')) as d_caricamento
        , testxml.annomese_sw
        , testxml.nome_flusso
FROM ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.TEST_XML testxml
LEFT JOIN ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_STORICI_RICONF_TABLE_NAME_PURGED} podriconf
    ON (testxml.pod14 = podriconf.pod_config)
WHERE podriconf.t_tipo_configurazione IS NULL or podriconf.t_tipo_configurazione <> 'SI';



