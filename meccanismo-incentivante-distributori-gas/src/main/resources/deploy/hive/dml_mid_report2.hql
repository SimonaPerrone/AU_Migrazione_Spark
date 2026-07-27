TRUNCATE TABLE ${hive.table.mid_report2};
WITH mid_agg AS (
	SELECT pdr,
		annomese,
		YEAR(data_tracciatura) AS anno_sterilizzazione,
		treatment AS trattamento,
		contatore AS n,
		CASE WHEN causale_tracciatura = 'E' THEN 'SI' ELSE 'NO' END AS sterilizzato_exc,
		CASE WHEN causale_tracciatura = 'I' THEN 'SI' ELSE 'NO' END AS sterilizzato_gdm,
		sessione_tracciatura AS sessione,
		executionid_daily_consumption
	FROM ${hive.table.mid_contatori} mid
	WHERE processo_tracciatura = 'AGG'
), agg AS (
    SELECT DISTINCT
        pdr,
        annomese,
        executionid,
        pivadistr,
        codremi,
        classemisuratore
    FROM ${hive.table.agg_daily_consumption}
    WHERE pivadistr IS NOT NULL
        AND pivaudd IS NOT NULL
        AND codremi IS NOT NULL
        AND classemisuratore IS NOT NULL
)
INSERT INTO ${hive.table.mid_report2}
SELECT
	mid.pdr AS pdr,
	mid.annomese AS annomese_sterilizzazione,
	mid.anno_sterilizzazione AS anno_sterilizzazione,
	mid.trattamento,
	mid.n,
	mid.sterilizzato_exc AS sterilizzato_exc,
	mid.sterilizzato_gdm AS sterilizzato_gdm,
	mid.sessione AS sessione,
	agg.pivadistr AS piva_dd,
	agg.codremi AS cod_remi,
	agg.classemisuratore AS gdm,
	az.t_rag_soc AS rag_soc_dd
FROM mid_agg AS mid
LEFT JOIN agg
ON mid.executionid_daily_consumption = agg.executionid AND mid.pdr = agg.pdr AND mid.annomese = agg.annomese
LEFT JOIN ${hive.table.rcu_azienda_p} az
ON agg.pivadistr = az.t_piva;

WITH mid_sbg AS (
	SELECT pdr,
		annomese,
		YEAR(data_tracciatura) AS anno_sterilizzazione,
		treatment AS trattamento,
		contatore AS n,
		CASE WHEN causale_tracciatura = 'E' THEN 'SI' ELSE 'NO' END AS sterilizzato_exc,
		CASE WHEN causale_tracciatura = 'I' THEN 'SI' ELSE 'NO' END AS sterilizzato_gdm,
		sessione_tracciatura AS sessione,
		executionid_daily_consumption
	FROM ${hive.table.mid_contatori} mid
	WHERE processo_tracciatura = 'SBG'
), sbg AS (
    SELECT DISTINCT
        pdr,
        annomese,
        executionid,
        pivadistr,
        codremi,
        classemisuratore
    FROM ${hive.table.sbg_daily_consumption}
    WHERE pivadistr IS NOT NULL
        AND pivaudd IS NOT NULL
        AND codremi IS NOT NULL
        AND classemisuratore IS NOT NULL
)
INSERT INTO ${hive.table.mid_report2}
SELECT
	mid.pdr AS pdr,
	mid.annomese AS annomese_sterilizzazione,
	mid.anno_sterilizzazione AS anno_sterilizzazione,
	mid.trattamento,
	mid.n,
	mid.sterilizzato_exc AS sterilizzato_exc,
	mid.sterilizzato_gdm AS sterilizzato_gdm,
	mid.sessione AS sessione,
	sbg.pivadistr AS piva_dd,
	sbg.codremi AS cod_remi,
	sbg.classemisuratore AS gdm,
	az.t_rag_soc AS rag_soc_dd
FROM mid_sbg AS mid
LEFT JOIN sbg
ON mid.executionid_daily_consumption = sbg.executionid AND mid.pdr = sbg.pdr AND mid.annomese = sbg.annomese
LEFT JOIN ${hive.table.rcu_azienda_p} az
ON sbg.pivadistr = az.t_piva;
