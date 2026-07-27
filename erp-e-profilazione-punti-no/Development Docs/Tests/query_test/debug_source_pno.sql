-- Verifica PNO nella tabella sorgente flusso_misure_noaggr
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

-- Verifica se il POD esiste in tratt_pod per anno=2025, mese=8
SELECT 
    'TRATT_POD_CHECK' as query_name,
    pod14,
    anno,
    mese,
    typeof(mese) as mese_type
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025
  AND mese = 8;

-- Simula il JOIN come nel codice per vedere se matcha
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
    CASE 
        WHEN t.pod14 IS NULL THEN 'NO_JOIN - POD not found in tratt_pod'
        WHEN lpad(CAST(f.mese AS STRING), 2, '0') = lpad(CAST(t.mese AS STRING), 2, '0') THEN 'JOIN_MATCH ✓'
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