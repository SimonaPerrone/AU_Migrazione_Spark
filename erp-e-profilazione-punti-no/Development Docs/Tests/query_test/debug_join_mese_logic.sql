-- ================================================
-- QUERY DI DEBUG: Analisi JOIN_MESE nel codice Scala
-- ================================================
-- Il problema è che il codice usa month(start_in) invece di annomese_in

-- SCENARIO: annomese_in = '202509' (SETTEMBRE 2025)
-- start_in = '20250831' (31 agosto - ultimo giorno mese precedente)
-- stop_in = '20250930' (30 settembre - ultimo giorno mese corrente)

SELECT 
    '1_LOGICA_SCALA_ERRATA' as scenario,
    f.pod,
    f.tipo_flusso,
    f.anno as flusso_anno,
    f.mese as flusso_mese,
    -- ❌ LOGICA SCALA ATTUALE (ERRATA): usa month(start_in)
    year(to_date('20250831', 'yyyyMMdd')) as join_anno_ERRATO,
    lpad(CAST(month(to_date('20250831', 'yyyyMMdd')) AS STRING), 2, '0') as join_mese_ERRATO,
    -- Dati tratt_pod
    t.anno as tratt_anno,
    lpad(CAST(t.mese AS STRING), 2, '0') as tratt_mese,
    t.pod14 as join_trovato,
    CASE 
        WHEN t.pod14 IS NULL THEN '❌ NO JOIN - cerca agosto ma hai solo agosto/settembre in tratt_pod'
        ELSE '✓ JOIN OK'
    END as result
FROM eng_test.flusso_misure_noaggr f
LEFT JOIN tratt_pod.tratt_pod_all_annomese_partitioned t
  ON f.pod = t.pod14
  AND t.anno = year(to_date('20250831', 'yyyyMMdd'))  -- 2025
  AND lpad(CAST(t.mese AS STRING), 2, '0') = lpad(CAST(month(to_date('20250831', 'yyyyMMdd')) AS STRING), 2, '0')  -- '08' ❌
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO'

UNION ALL

SELECT 
    '2_LOGICA_CORRETTA_SPEC' as scenario,
    f.pod,
    f.tipo_flusso,
    f.anno as flusso_anno,
    f.mese as flusso_mese,
    -- ✅ LOGICA CORRETTA SECONDO SPEC: usa annomese_in
    CAST(substring('202509', 1, 4) AS INT) as join_anno_CORRETTO,
    substring('202509', 5, 2) as join_mese_CORRETTO,
    -- Dati tratt_pod
    t.anno as tratt_anno,
    lpad(CAST(t.mese AS STRING), 2, '0') as tratt_mese,
    t.pod14 as join_trovato,
    CASE 
        WHEN t.pod14 IS NULL THEN '❌ NO JOIN'
        ELSE '✓ JOIN OK - cerca settembre e lo trova!'
    END as result
FROM eng_test.flusso_misure_noaggr f
LEFT JOIN tratt_pod.tratt_pod_all_annomese_partitioned t
  ON f.pod = t.pod14
  AND t.anno = CAST(substring('202509', 1, 4) AS INT)  -- 2025
  AND lpad(CAST(t.mese AS STRING), 2, '0') = substring('202509', 5, 2)  -- '09' ✅
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO'

UNION ALL

-- Verifica: quali mesi hai in tratt_pod per questo POD?
SELECT 
    '3_MESI_DISPONIBILI' as scenario,
    pod14 as pod,
    NULL as tipo_flusso,
    anno as flusso_anno,
    mese as flusso_mese,
    NULL as join_anno_ERRATO,
    NULL as join_mese_ERRATO,
    anno as tratt_anno,
    lpad(CAST(mese AS STRING), 2, '0') as tratt_mese,
    pod14 as join_trovato,
    CONCAT('Hai tratt_pod per: ', anno, '-', lpad(CAST(mese AS STRING), 2, '0')) as result
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025
ORDER BY scenario, tratt_anno, tratt_mese;
