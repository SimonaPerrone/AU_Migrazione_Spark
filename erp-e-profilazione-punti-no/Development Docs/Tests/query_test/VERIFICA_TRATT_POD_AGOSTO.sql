-- ================================================================
-- VERIFICA DEFINITIVA: Il POD esisteva in tratt_pod per agosto?
-- ================================================================

-- PARTE 1: Tutti i mesi disponibili per questo POD in tratt_pod
SELECT 
    '1_MESI_DISPONIBILI_TRATT_POD' as step,
    pod14,
    CAST(anno AS STRING) as anno,
    CAST(mese AS STRING) as mese,
    CAST(NULL AS STRING) as col5,
    CAST(NULL AS STRING) as col6,
    CAST(NULL AS STRING) as col7,
    CONCAT('POD disponibile in tratt_pod per anno=', CAST(anno AS STRING), ' mese=', CAST(mese AS STRING)) as note
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025

UNION ALL

-- PARTE 2: Record PNO con mese=8 in flusso_misure_noaggr
SELECT 
    '2_RECORD_PNO_MESE_8' as step,
    pod,
    CAST(anno AS STRING) as anno,
    CAST(mese AS STRING) as mese,
    trattamento,
    data_misura,
    nomefile,
    CONCAT('PNO data_misura=31/08, mese=8, elaborato il ', dataelaborazione) as note
FROM eng_test.flusso_misure_noaggr
WHERE pod = 'IT001E04533765'
  AND tipo_flusso = 'PNO'
  AND anno = 2025
  AND mese = 8
LIMIT 1

UNION ALL

-- PARTE 3: Simula JOIN come fa Scala per PNO (usando mese=8 da start_in)
SELECT 
    '3_JOIN_SCALA_PNO_MESE_8' as step,
    f.pod,
    CAST(f.anno AS STRING) as anno,
    lpad(CAST(f.mese AS STRING), 2, '0') as mese,
    f.trattamento,
    CAST(NULL AS STRING) as col6,
    CAST(NULL AS STRING) as col7,
    CASE 
        WHEN t.pod14 IS NULL THEN '❌ NO JOIN - POD non trovato in tratt_pod per agosto!'
        ELSE '✓ JOIN OK'
    END as note
FROM eng_test.flusso_misure_noaggr f
LEFT JOIN tratt_pod.tratt_pod_all_annomese_partitioned t
  ON f.pod = t.pod14
  AND f.anno = t.anno
  AND lpad(CAST(f.mese AS STRING), 2, '0') = lpad(CAST(t.mese AS STRING), 2, '0')
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO'
  AND f.mese = 8
LIMIT 1;

-- ================================================================
-- IPOTESI:
-- ================================================================
-- Se PARTE 3 mostra "NO JOIN", significa che:
-- - Il POD IT001E04533765 NON esisteva in tratt_pod per agosto 2025
-- - Oppure esisteva ma con anno/mese diverso
-- - Il codice Scala lo ha quindi scartato correttamente!
-- 
-- Se PARTE 3 mostra "JOIN OK", allora il problema è ALTROVE
-- nella pipeline (filtri successivi, validazioni, ecc.)
