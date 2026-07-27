DROP TABLE IF EXISTS ${hive.table.misure_storic_f2};
DROP VIEW IF EXISTS ${hive.table.misure_storic_f2};
CREATE VIEW ${hive.table.misure_storic_f2} AS
SELECT
	codice_fiscale AS cf_piva,
	codice_pdr AS pdr,
	annomese AS annomese_riferimento,
	data_lettura,
	SUBSTR(CAST(data_caricamento AS STRING), 1 ,19) AS dt_caricamento,
	CASE
		WHEN flusso IN ('tal', 'tav') THEN 'Autolettura'
		WHEN flusso IN ('tml', 'tgl') THEN 'Lettura Periodica'
		WHEN flusso IN ('rml', 'rgl') THEN 'Lettura di Rettifica'
		WHEN flusso IN ('vtg') THEN 'Lettura Voltura'
		WHEN flusso IN ('rmv') THEN 'Lettura di Rettifica Voltura'
		ELSE ''
	END AS flusso,
	CASE
		WHEN motivazione = 1 THEN 'Misura che sostituisce una stima precedente.'
		WHEN motivazione = 2 THEN 'Misura che sostituisce una misura fornita precedentemente errata.'
		WHEN motivazione = 3 THEN 'Misura fornita precedentemente per errore.'
		WHEN motivazione = 4 THEN 'Ricostruzione per frode.'
		WHEN motivazione = 5 THEN 'Ricostruzione per malfunzionamento misuratore.'
		ELSE ''
	END AS motivazione,
	LPAD(CAST(lettura AS STRING), 9, '0') AS let_tot_prel,
	SUBSTRING(codice_pdr, 7, 3) AS cod_pdr
FROM misuregas.forniture_misure_gas
WHERE riempimento = 0 AND codice_fiscale IS NOT NULL;
