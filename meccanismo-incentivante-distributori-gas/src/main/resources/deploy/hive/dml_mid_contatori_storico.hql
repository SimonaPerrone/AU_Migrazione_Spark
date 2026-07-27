-- Popola la tabella dei contatori con lo storico fornito sui mesi passati (usato solo al rilascio)
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ${hive.table.mid_contatori} PARTITION (annomese, executionid_tracciatura)
SELECT
    ie.pdr AS pdr,
    1 AS contatore,
    'V' AS stato,
    sbg.treatment AS treatment,
    CAST(current_timestamp() AS DATE) AS data_tracciatura,
    'SBG' AS processo_tracciatura,
    'SBG' AS sessione_tracciatura,
    ie.causale_tracciatura AS causale_tracciatura,
    'ORDINARIO' AS tipo_calcolo,
    sbg.executionid AS executionid_daily_consumption,
    CAST(null AS BIGINT) AS executionid_tracciatura_prev,
    ie.annomese AS annomese,
    CAST(unix_timestamp() * 1000 AS BIGINT) AS executionid_tracciatura
FROM (
    SELECT cod_pdr AS pdr, annomese, executionid, 'E' AS causale_tracciatura FROM atg_bis.pdr_inc_exc_sbg_mid
    UNION ALL
    SELECT cod_pdr AS pdr, annomese, executionid, 'I' AS causale_tracciatura FROM atg_bis.pdr_inc_gdm_sbg_mid
    ) AS ie
LEFT JOIN (
    SELECT pdr, annomese, treatment, executionid
    FROM sbg.daily_consumption_sbg
    GROUP BY pdr, annomese, treatment, executionid
) AS sbg
ON ie.pdr = sbg.pdr AND ie.annomese = sbg.annomese AND ie.executionid = sbg.executionid;
