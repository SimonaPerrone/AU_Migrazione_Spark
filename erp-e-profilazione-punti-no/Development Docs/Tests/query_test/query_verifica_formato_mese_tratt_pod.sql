-- Query per verificare il formato della colonna MESE in tratt_pod
-- per capire se è '8' o '08', '9' o '09'

-- ============================================================
-- 1) VERIFICA FORMATO MESE in TRATT_POD per il POD specifico
-- ============================================================
SELECT 
  pod14,
  anno,
  mese,
  CAST(mese AS STRING) as mese_string,
  LENGTH(CAST(mese AS STRING)) as len_mese,
  CASE 
    WHEN LENGTH(CAST(mese AS STRING)) = 1 THEN 'formato_single_digit'
    WHEN LENGTH(CAST(mese AS STRING)) = 2 THEN 'formato_double_digit'
    ELSE 'formato_unknown'
  END as tipo_formato,
  is_t_trattamento
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025
  AND (mese = 8 OR mese = 9)
ORDER BY anno, mese;

-- ============================================================
-- 2) CAMPIONE GENERALE: vediamo i formati di mese nella tabella
-- ============================================================
SELECT 
  mese,
  CAST(mese AS STRING) as mese_string,
  LENGTH(CAST(mese AS STRING)) as len_mese,
  COUNT(*) as num_record,
  CASE 
    WHEN LENGTH(CAST(mese AS STRING)) = 1 THEN 'formato_single_digit'
    WHEN LENGTH(CAST(mese AS STRING)) = 2 THEN 'formato_double_digit'
    ELSE 'formato_unknown'
  END as tipo_formato
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE anno = 2025
GROUP BY mese
ORDER BY mese;

-- ============================================================
-- 3) VERIFICA PRESENZA POD in TRATT_POD per anno/mese corretti
-- ============================================================
-- Controlliamo se il POD ha entry per agosto (8) e settembre (9)
SELECT 
  pod14,
  anno,
  mese,
  is_t_trattamento,
  COUNT(*) as num_righe
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025
GROUP BY pod14, anno, mese, is_t_trattamento
ORDER BY anno, mese;

-- ============================================================
-- 4) SIMULAZIONE JOIN: vediamo se il join funzionerebbe
-- ============================================================
-- Simuliamo il join tra flusso_misure_noaggr e tratt_pod
-- per vedere se troviamo match per il PNO con mese=8

-- Prima vediamo cosa c'è in flusso_misure_noaggr per PNO
SELECT 
  'flusso_misure' as source,
  pod,
  tipo_flusso,
  anno,
  mese,
  CAST(mese AS STRING) as mese_string,
  LENGTH(CAST(mese AS STRING)) as len_mese,
  data_misura
FROM eng_test.flusso_misure_noaggr
WHERE pod = 'IT001E04533765'
  AND UPPER(TRIM(tipo_flusso)) = 'PNO'
  AND anno = 2025

UNION ALL

-- Poi vediamo cosa c'è in tratt_pod
SELECT 
  'tratt_pod' as source,
  pod14 as pod,
  NULL as tipo_flusso,
  anno,
  mese,
  CAST(mese AS STRING) as mese_string,
  LENGTH(CAST(mese AS STRING)) as len_mese,
  NULL as data_misura
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE pod14 = 'IT001E04533765'
  AND anno = 2025
  AND mese = 8

ORDER BY source, mese;

-- ============================================================
-- 5) VERIFICA DISTINCT mese values nella tabella tratt_pod
-- ============================================================
SELECT DISTINCT
  mese,
  CAST(mese AS STRING) as mese_string,
  LENGTH(CAST(mese AS STRING)) as len_mese,
  CASE 
    WHEN LENGTH(CAST(mese AS STRING)) = 1 THEN 'single_digit'
    WHEN LENGTH(CAST(mese AS STRING)) = 2 THEN 'double_digit'
    ELSE 'other'
  END as pattern
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE anno = 2025
ORDER BY mese;
