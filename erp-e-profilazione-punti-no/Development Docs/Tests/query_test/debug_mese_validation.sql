-- ================================================
-- QUERY DI DEBUG: Verifica validazione campo MESE
-- ================================================
-- Controlla se il POD viene scartato dalla validazione

SELECT 
    'MESE_CHECK' as query_name,
    f.pod,
    f.tipo_flusso,
    f.anno,
    f.mese,
    f.mese IS NULL as mese_is_null,
    CASE 
        WHEN f.mese IS NULL THEN 'SCARTATO - mese NULL ❌'
        ELSE 'VALIDO ✓'
    END as validation_result,
    t.pod14 as tratt_pod_exists,
    CASE 
        WHEN t.pod14 IS NULL THEN 'SCARTATO - POD non trovato ❌'
        WHEN f.mese IS NULL THEN 'SCARTATO - mese NULL ❌'
        ELSE 'PASSA VALIDAZIONE ✓'
    END as final_validation
FROM eng_test.flusso_misure_noaggr f
LEFT JOIN tratt_pod.tratt_pod_all_annomese_partitioned t
  ON f.pod = t.pod14
  AND f.anno = t.anno
  AND lpad(CAST(f.mese AS STRING), 2, '0') = lpad(CAST(t.mese AS STRING), 2, '0')
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO'
  AND f.anno = 2025;
