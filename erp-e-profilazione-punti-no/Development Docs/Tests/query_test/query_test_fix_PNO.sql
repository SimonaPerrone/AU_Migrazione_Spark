-- Query di test per verificare il FIX del PNO
-- POD: IT001E04533765
-- ExecutionID: 1762338130001
-- Atteso: PNO con data_misura='2025-08-31' e mese='08'

-- ============================================================
-- 1) VERIFICA PRESENZA PNO in erp_validated_mis_no
--    Deve esserci almeno 1 riga PNO con data 31/08/2025
-- ============================================================
SELECT 
  pod,
  tipo_flusso,
  data_misura,
  anno,
  mese,
  executionid,
  CASE 
    WHEN data_misura = '2025-08-31' AND tipo_flusso = 'PNO' THEN '✅ PNO_TROVATO'
    ELSE 'PNO2G o altra data'
  END as check_fix
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762338130001'
  AND tipo_flusso IN ('PNO', 'PNO2G')
ORDER BY data_misura, tipo_flusso;

-- ============================================================
-- 2) CONTA PNO vs PNO2G per questo executionid
-- ============================================================
SELECT 
  tipo_flusso,
  COUNT(*) as num_righe,
  MIN(data_misura) as prima_data,
  MAX(data_misura) as ultima_data,
  CASE 
    WHEN tipo_flusso = 'PNO' AND COUNT(*) >= 1 THEN '✅ PNO_OK'
    WHEN tipo_flusso = 'PNO2G' AND COUNT(*) >= 4 THEN '✅ PNO2G_OK'
    ELSE '⚠️ VERIFICARE'
  END as status
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762338130001'
  AND tipo_flusso IN ('PNO', 'PNO2G')
GROUP BY tipo_flusso
ORDER BY tipo_flusso;

-- ============================================================
-- 3) VERIFICA FORMATO MESE (deve essere '08' non '8')
-- ============================================================
SELECT 
  tipo_flusso,
  data_misura,
  anno,
  mese,
  LENGTH(mese) as len_mese,
  CASE 
    WHEN LENGTH(mese) = 2 THEN '✅ formato_ok'
    WHEN LENGTH(mese) = 1 THEN '❌ formato_single_digit'
    ELSE '⚠️ formato_unknown'
  END as check_formato_mese
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762338130001'
  AND tipo_flusso IN ('PNO', 'PNO2G')
ORDER BY data_misura, tipo_flusso;

-- ============================================================
-- 4) VERIFICA PRESENZA in erp_consumption_no
--    Controlliamo se il PNO è stato elaborato nei consumi
-- ============================================================
SELECT 
  pod,
  tipo_flusso_sx,
  tipo_flusso_dx,
  data_misura_sx,
  data_misura_dx,
  stato,
  executionid
FROM eng_test.erp_consumption_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762338130001'
  AND (tipo_flusso_sx = 'PNO' OR tipo_flusso_dx = 'PNO')
ORDER BY data_misura_sx, data_misura_dx
LIMIT 20;

-- ============================================================
-- 5) VERIFICA PRESENZA in erp_daily_no
--    Il PNO deve generare righe giornaliere profilate
-- ============================================================
SELECT 
  pod,
  giorno,
  anno,
  mese,
  LENGTH(mese) as len_mese,
  area,
  piva_distr,
  rag_soc_distr,
  executionid
FROM eng_test.erp_daily_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762338130001'
  AND anno = '2025'
  AND mese IN ('08', '8')  -- cerca sia con che senza zero-padding
ORDER BY giorno
LIMIT 50;

-- ============================================================
-- 6) CONTA TOTALE RIGHE per il POD in questo executionid
-- ============================================================
SELECT 
  'erp_validated_mis_no' as tabella,
  COUNT(*) as num_righe
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762338130001'

UNION ALL

SELECT 
  'erp_consumption_no' as tabella,
  COUNT(*) as num_righe
FROM eng_test.erp_consumption_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762338130001'

UNION ALL

SELECT 
  'erp_daily_no' as tabella,
  COUNT(*) as num_righe
FROM eng_test.erp_daily_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762338130001'

UNION ALL

SELECT 
  'erp_aggregato_no' as tabella,
  COUNT(*) as num_righe
FROM eng_test.erp_aggregato_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762338130001';

-- ============================================================
-- 7) CONFRONTO: executionid VECCHIO vs NUOVO
--    Vediamo la differenza tra il run precedente e questo
-- ============================================================
SELECT 
  executionid,
  tipo_flusso,
  COUNT(*) as num_righe,
  MIN(data_misura) as prima_data,
  MAX(data_misura) as ultima_data
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND tipo_flusso IN ('PNO', 'PNO2G')
  AND executionid IN ('1762281417733', '1762338130001')  -- vecchio vs nuovo
GROUP BY executionid, tipo_flusso
ORDER BY executionid DESC, tipo_flusso;

-- ============================================================
-- 8) QUERY SPECIFICA: cerca ESATTAMENTE il PNO del 31/08/2025
-- ============================================================
SELECT 
  pod,
  tipo_flusso,
  data_misura,
  anno,
  mese,
  validato,
  tipo_dato_e,
  executionid,
  '✅ PNO TROVATO CON SUCCESSO!' as risultato
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND tipo_flusso = 'PNO'
  AND data_misura = '2025-08-31'
  AND executionid = '1762338130001';
