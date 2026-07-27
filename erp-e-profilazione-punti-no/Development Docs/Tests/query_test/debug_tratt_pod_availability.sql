-- ================================================
-- QUERY DI DEBUG: Verifica disponibilità tratt_pod
-- ================================================
-- Controlla quali mesi hai disponibili in tratt_pod per il POD

SELECT 
    'TRATT_POD_MONTHS' as query_name,
    pod14,
    anno,
    mese,
    lpad(CAST(mese AS STRING), 2, '0') as mese_padded,
    is_t_trattamento
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025
ORDER BY anno, mese;

-- ================================================
-- Test JOIN con diversi scenari start_in
-- ================================================

-- SCENARIO 1: start_in = '20250731' (31 luglio)
SELECT 
    'SCENARIO_LUGLIO' as scenario,
    f.pod,
    f.tipo_flusso,
    f.anno as source_anno,
    f.mese as source_mese,
    -- Logica Scala ATTUALE (ERRATA)
    2025 as join_anno_from_start_in,
    '07' as join_mese_from_start_in,
    -- Dati tratt_pod
    t.anno as tratt_anno,
    t.mese as tratt_mese,
    lpad(CAST(t.mese AS STRING), 2, '0') as tratt_mese_padded,
    t.pod14 as join_trovato,
    CASE 
        WHEN t.pod14 IS NULL THEN '❌ NO JOIN - tratt_pod non ha luglio'
        ELSE '✓ JOIN OK'
    END as result
FROM eng_test.flusso_misure_noaggr f
LEFT JOIN tratt_pod.tratt_pod_all_annomese_partitioned t
  ON f.pod = t.pod14
  AND t.anno = 2025  -- year(start_in='20250731')
  AND lpad(CAST(t.mese AS STRING), 2, '0') = '07'  -- month(start_in)
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO'

UNION ALL

-- SCENARIO 2: start_in = '20250831' (31 agosto) - CORRETTO
SELECT 
    'SCENARIO_AGOSTO' as scenario,
    f.pod,
    f.tipo_flusso,
    f.anno as source_anno,
    f.mese as source_mese,
    -- Logica Scala con start_in corretto
    2025 as join_anno_from_start_in,
    '08' as join_mese_from_start_in,
    -- Dati tratt_pod
    t.anno as tratt_anno,
    t.mese as tratt_mese,
    lpad(CAST(t.mese AS STRING), 2, '0') as tratt_mese_padded,
    t.pod14 as join_trovato,
    CASE 
        WHEN t.pod14 IS NULL THEN '❌ NO JOIN'
        ELSE '✓ JOIN OK - tratt_pod ha agosto'
    END as result
FROM eng_test.flusso_misure_noaggr f
LEFT JOIN tratt_pod.tratt_pod_all_annomese_partitioned t
  ON f.pod = t.pod14
  AND t.anno = 2025  -- year(start_in='20250831')
  AND lpad(CAST(t.mese AS STRING), 2, '0') = '08'  -- month(start_in)
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO';
