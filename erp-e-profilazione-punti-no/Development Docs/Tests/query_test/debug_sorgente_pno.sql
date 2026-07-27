-- ============================================
-- DEBUG PNO - Verifica is_t_trattamento
-- ============================================

-- Query 1: Verifica PNO nella tabella sorgente flusso_misure_noaggr
SELECT 
    'SOURCE_PNO' as query_name,
    pod,
    tipo_flusso,
    data_misura,
    anno,
    mese,
    typeof(mese) as mese_type
FROM eng_test.flusso_misure_noaggr
WHERE pod = 'IT001E04533765'
  AND tipo_flusso = 'PNO'
  AND anno = 2025;

-- Query 2: Verifica POD in tratt_pod CON is_t_trattamento
-- QUESTA È LA QUERY CRITICA: controlla il valore di is_t_trattamento
SELECT 
    'TRATT_POD_CHECK' as query_name,
    pod14,
    anno,
    mese,
    typeof(mese) as mese_type,
    is_t_trattamento,
    typeof(is_t_trattamento) as is_t_trattamento_type,
    CASE 
        WHEN is_t_trattamento IN ('M', 'F', 'C') THEN 'OK ✓ - verrà processato'
        ELSE 'FAIL ✗ - verrà ESCLUSO dal filtro!'
    END as filter_check
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025
  AND mese = 8;

-- Query 3: Simula il JOIN completo con controllo is_t_trattamento
SELECT 
    'JOIN_SIMULATION' as query_name,
    f.pod,
    f.tipo_flusso,
    f.anno as source_anno,
    f.mese as source_mese,
    t.anno as tratt_anno,
    t.mese as tratt_mese,
    lpad(CAST(t.mese AS STRING), 2, '0') as tratt_mese_padded,
    lpad(CAST(f.mese AS STRING), 2, '0') as source_mese_padded,
    t.is_t_trattamento,
    CASE 
        WHEN t.pod14 IS NULL THEN 'NO_JOIN - POD not found in tratt_pod'
        WHEN t.is_t_trattamento NOT IN ('M', 'F', 'C') THEN 'JOIN OK ma FILTRATO - is_t_trattamento NON valido!'
        WHEN lpad(CAST(f.mese AS STRING), 2, '0') = lpad(CAST(t.mese AS STRING), 2, '0') THEN 'JOIN_MATCH ✓ e filtro OK'
        ELSE 'JOIN_FAIL - mese mismatch'
    END as join_result
FROM eng_test.flusso_misure_noaggr f
LEFT JOIN tratt_pod.tratt_pod_all_annomese_partitioned t
  ON f.pod = t.pod14
  AND f.anno = t.anno
  AND lpad(CAST(f.mese AS STRING), 2, '0') = lpad(CAST(t.mese AS STRING), 2, '0')
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO'
  AND f.anno = 2025;

-- Query 4: Simula il filtro esatto del codice Scala (linea 157)
-- Questo è esattamente ciò che fa il codice prima del JOIN
SELECT 
    'FILTER_SIMULATION' as query_name,
    pod14,
    anno,
    mese,
    is_t_trattamento,
    COUNT(*) as record_count,
    'Questi sono i POD che PASSANO il filtro is_t_trattamento' as note
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025
  AND mese = 8
  AND UPPER(is_t_trattamento) IN ('M', 'F', 'C')
GROUP BY pod14, anno, mese, is_t_trattamento;