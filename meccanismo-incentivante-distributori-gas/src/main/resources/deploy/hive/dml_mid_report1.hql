TRUNCATE TABLE ${hive.table.mid_report1};
-- agg con executionId passato da parametro
WITH agg AS (
    SELECT DISTINCT
        pdr,
        annomese,
        pivadistr AS piva_dd,
        codremi AS cod_remi,
        classemisuratore AS gdm
    FROM ${hive.table.agg_daily_consumption}
    WHERE executionid = ${hivevar:executionId}
        AND pivadistr IS NOT NULL
        AND pivaudd IS NOT NULL
        AND codremi IS NOT NULL
        AND classemisuratore IS NOT NULL
),
-- elementi di mid con massima esecuzione per annomese in perimetro
mid AS (
    SELECT *
    FROM (
        SELECT pdr,
            annomese,
            stato,
            treatment AS trattamento,
            contatore AS n,
            executionid_tracciatura,
            MAX(executionid_tracciatura) OVER (PARTITION BY annomese) AS max_execid_annomese
        FROM ${hive.table.mid_contatori} mid
        WHERE mid.annomese IN (
            SELECT DISTINCT annomese
            FROM agg
        )
    ) as mid1
    WHERE executionid_tracciatura = max_execid_annomese and stato != 'I'
)
INSERT INTO ${hive.table.mid_report1}
SELECT
	mid.pdr AS pdr,
	mid.annomese AS annomese,
	mid.trattamento AS trattamento,
	mid.n AS n,
	agg.piva_dd AS piva_dd,
	agg.cod_remi AS cod_remi,
	agg.gdm AS gdm,
	az.t_rag_soc AS rag_soc_dd
FROM mid
LEFT JOIN agg
ON mid.pdr = agg.pdr AND mid.annomese = agg.annomese
LEFT JOIN ${hive.table.rcu_azienda_p} az
ON agg.piva_dd = az.t_piva;
