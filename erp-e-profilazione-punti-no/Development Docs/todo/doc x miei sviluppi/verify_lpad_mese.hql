-- =====================================================
-- VERIFICA LPAD su MESE - Tutte le tabelle ERP
-- =====================================================
-- Data: 2025-11-13
-- Obiettivo: Verificare che il campo 'mese' sia correttamente
--            formattato con zero-padding (es. "09" invece di "9")
-- =====================================================

USE eng_test;

-- =====================================================
-- 1. ERP_VALIDATED_MIS_NO (validated measures)
-- =====================================================
SELECT 
  'erp_validated_mis_no' AS tabella,
  mese,
  COUNT(*) AS cnt,
  LENGTH(mese) AS lunghezza_mese,
  CASE 
    WHEN LENGTH(mese) = 1 THEN '❌ MANCA PADDING'
    WHEN LENGTH(mese) = 2 THEN '✅ PADDING OK'
    ELSE '⚠️ FORMATO ANOMALO'
  END AS check_padding
FROM eng_test.erp_validated_mis_no
WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_validated_mis_no)
GROUP BY mese
ORDER BY mese;

-- =====================================================
-- 2. ERP_DAILY_NO (daily profiles)
-- =====================================================
SELECT 
  'erp_daily_no' AS tabella,
  mese,
  COUNT(*) AS cnt,
  CASE 
    WHEN CAST(mese AS STRING) = mese THEN LENGTH(CAST(mese AS STRING))
    ELSE NULL
  END AS lunghezza_mese,
  CASE 
    WHEN LENGTH(CAST(mese AS STRING)) = 1 THEN '❌ MANCA PADDING (INT 9)'
    WHEN LENGTH(CAST(mese AS STRING)) = 2 THEN '✅ PADDING OK (STRING 09)'
    ELSE '⚠️ FORMATO ANOMALO'
  END AS check_padding
FROM eng_test.erp_daily_no
WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_daily_no)
GROUP BY mese
ORDER BY mese;

-- =====================================================
-- 3. ERP_AGGREGATO_NO (aggregated data)
-- =====================================================
SELECT 
  'erp_aggregato_no' AS tabella,
  mese,
  COUNT(*) AS cnt,
  LENGTH(mese) AS lunghezza_mese,
  CASE 
    WHEN LENGTH(mese) = 1 THEN '❌ MANCA PADDING'
    WHEN LENGTH(mese) = 2 THEN '✅ PADDING OK'
    ELSE '⚠️ FORMATO ANOMALO'
  END AS check_padding
FROM eng_test.erp_aggregato_no
WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_aggregato_no)
GROUP BY mese
ORDER BY mese;

-- =====================================================
-- 4. Verifica JOIN con TRATT_POD (problema AV2G/DS2G)
-- =====================================================
-- Controlla se i flussi tecnici matchano correttamente con tratt_pod
SELECT 
  v.tipo_flusso,
  v.mese AS mese_validated,
  COUNT(DISTINCT v.pod) AS pod_count_validated,
  COUNT(DISTINCT t.pod14) AS pod_count_tratt,
  CASE 
    WHEN COUNT(DISTINCT t.pod14) > 0 THEN '✅ JOIN OK'
    ELSE '❌ JOIN FALLITA'
  END AS check_join
FROM eng_test.erp_validated_mis_no v
LEFT JOIN eng_test.tratt_pod_all_annomese_partitioned t
  ON v.pod = t.pod14 
  AND v.anno = t.anno
  AND v.mese = LPAD(CAST(t.mese AS STRING), 2, '0')  -- JOIN con LPAD
WHERE v.executionid = (SELECT MAX(executionid) FROM eng_test.erp_validated_mis_no)
  AND UPPER(v.tipo_flusso) IN ('AV', 'AV2G', 'DS', 'DS2G')  -- Flussi tecnici
GROUP BY v.tipo_flusso, v.mese
ORDER BY v.tipo_flusso, v.mese;

-- =====================================================
-- 5. Riepilogo generale per ultima esecuzione
-- =====================================================
SELECT 
  'RIEPILOGO ULTIMA ESECUZIONE' AS info,
  (SELECT MAX(executionid) FROM eng_test.erp_validated_mis_no) AS execution_id,
  (SELECT COUNT(DISTINCT mese) FROM eng_test.erp_validated_mis_no 
   WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_validated_mis_no)) AS mesi_validated,
  (SELECT COUNT(*) FROM eng_test.erp_validated_mis_no 
   WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_validated_mis_no)) AS record_validated,
  (SELECT COUNT(*) FROM eng_test.erp_daily_no 
   WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_daily_no)) AS record_daily;

-- =====================================================
-- 6. Verifica specifica: POD IT018E00749251
-- =====================================================
-- Controlla se il POD problematico ha ancora duplicati
SELECT 
  pod,
  data_misura,
  tipo_flusso,
  mese,
  time_stamp,
  executionid
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT018E00749251'
  AND executionid = (SELECT MAX(executionid) FROM eng_test.erp_validated_mis_no)
ORDER BY data_misura, time_stamp DESC;
