-- ================================================================
-- DIAGNOSI FINALE: Perché IT001E04533765 PNO non appare in erp_validated_mis
-- ================================================================

-- DATI DI PARTENZA
-- flusso_misure_noaggr: POD IT001E04533765, tipo_flusso=PNO, anno=2025, mese=8
-- tratt_pod: Hai solo mesi 8 (agosto) e 9 (settembre) per anno 2025

-- ================================================================
-- PARTE 1: Verifica mesi disponibili in tratt_pod
-- ================================================================
SELECT 
    '1_TRATT_POD_DISPONIBILE' as step,
    pod14 as pod,
    anno,
    mese,
    lpad(CAST(mese AS STRING), 2, '0') as mese_padded,
    'Questi sono i mesi disponibili in tratt_pod per il POD' as note
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025
ORDER BY anno, mese

UNION ALL

-- ================================================================
-- PARTE 2: Dati in flusso_misure_noaggr
-- ================================================================
SELECT 
    '2_FLUSSO_MISURE_NOAGGR' as step,
    pod,
    anno,
    mese,
    lpad(CAST(mese AS STRING), 2, '0') as mese_padded,
    CONCAT('Record PNO con mese=', mese, ' (agosto)') as note
FROM eng_test.flusso_misure_noaggr
WHERE pod = 'IT001E04533765'
  AND tipo_flusso = 'PNO'
  AND anno = 2025
LIMIT 1

UNION ALL

-- ================================================================
-- PARTE 3: LOGICA SCALA ATTUALE (ERRATA)
-- Simula: annomese_in='202509' → start_in='20250831'
-- Il codice fa JOIN con year(start_in)=2025, month(start_in)=08
-- ================================================================
SELECT 
    '3_SCALA_USA_START_IN' as step,
    f.pod,
    year(to_date('20250831', 'yyyyMMdd')) as join_anno,
    lpad(CAST(month(to_date('20250831', 'yyyyMMdd')) AS STRING), 2, '0') as join_mese,
    lpad(CAST(month(to_date('20250831', 'yyyyMMdd')) AS STRING), 2, '0') as mese_padded,
    CASE 
        WHEN t.pod14 IS NOT NULL THEN 'JOIN OK ✓'
        ELSE 'JOIN FAIL ❌ - tratt_pod ha mese 8 MA il record flusso ha mese 8!'
    END as note
FROM eng_test.flusso_misure_noaggr f
LEFT JOIN tratt_pod.tratt_pod_all_annomese_partitioned t
  ON f.pod = t.pod14
  AND t.anno = year(to_date('20250831', 'yyyyMMdd'))  -- 2025
  AND lpad(CAST(t.mese AS STRING), 2, '0') = lpad(CAST(month(to_date('20250831', 'yyyyMMdd')) AS STRING), 2, '0')  -- '08'
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO'
LIMIT 1

UNION ALL

-- ================================================================
-- PARTE 4: VERIFICA - Il problema NON è il tipo di dato del mese
-- ================================================================
SELECT 
    '4_VERIFICA_TIPO_MESE' as step,
    f.pod,
    f.anno,
    f.mese as mese_INT,
    CAST(f.mese AS STRING) as mese_STRING,
    lpad(CAST(f.mese AS STRING), 2, '0') as mese_PADDED,
    CONCAT('flusso.mese è INT=', f.mese, ', tratt_pod.mese è anche INT') as note
FROM eng_test.flusso_misure_noaggr f
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO'
LIMIT 1

UNION ALL

-- ================================================================
-- PARTE 5: TEST - Se usassimo f.anno e f.mese FUNZIONEREBBE?
-- ================================================================
SELECT 
    '5_TEST_CON_F_MESE' as step,
    f.pod,
    f.anno as join_anno,
    f.mese as join_mese_raw,
    lpad(CAST(f.mese AS STRING), 2, '0') as mese_padded,
    CASE 
        WHEN t.pod14 IS NOT NULL THEN 'JOIN OK ✓ - Usando f.mese funziona!'
        ELSE 'JOIN FAIL ❌'
    END as note
FROM eng_test.flusso_misure_noaggr f
LEFT JOIN tratt_pod.tratt_pod_all_annomese_partitioned t
  ON f.pod = t.pod14
  AND t.anno = f.anno  -- Usa anno dalla tabella
  AND lpad(CAST(t.mese AS STRING), 2, '0') = lpad(CAST(f.mese AS STRING), 2, '0')  -- Usa mese dalla tabella
WHERE f.pod = 'IT001E04533765'
  AND f.tipo_flusso = 'PNO'
LIMIT 1;

-- ================================================================
-- CONCLUSIONE ATTESA:
-- ================================================================
-- Step 1: tratt_pod ha mesi 8 e 9 disponibili
-- Step 2: flusso_misure_noaggr ha record con mese=8
-- Step 3: Scala usa start_in ('20250831') → month=08 → JOIN dovrebbe funzionare!
-- Step 4: Conferma che i tipi di dato sono corretti
-- Step 5: Test alternativo - conferma che usando f.mese funziona

-- SE IL JOIN NON FUNZIONA NEMMENO QUI, il problema è ALTROVE!
-- Possibili cause:
-- 1. Il job è stato eseguito con start_in DIVERSO (es. '20250731'=luglio)
-- 2. C'è un altro filtro nascosto nel codice che scarta il record
-- 3. Il record viene perso in un'altra fase della pipeline
