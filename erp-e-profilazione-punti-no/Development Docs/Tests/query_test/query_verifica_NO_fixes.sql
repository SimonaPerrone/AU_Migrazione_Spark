-- ===========================================
-- VERIFICA FIX NO FLOW - EXECUTION ID: 1762270210426
-- ===========================================

-- 1. TROVA L'EXECUTION ID PIU' RECENTE
SELECT
  executionid,
  anno,
  mese,
  COUNT(*) as num_record
FROM eng_test.erp_validated_mis_no
WHERE anno = 2025 AND mese = 9
GROUP BY executionid, anno, mese
ORDER BY executionid DESC
LIMIT 10;

-- 2. VERIFICA FIX DATA PNO2G (dovrebbe essere 2025-08-31, non 2025-08-30)
SELECT
  pod,
  tipo_flusso_sx,
  tipo_flusso_dx,
  data_misura_sx,
  data_misura_dx,
  anno,
  mese,
  executionid
FROM eng_test.erp_validated_mis_no
WHERE executionid = 1762270210426
  AND anno = 2025
  AND mese = 9
  AND pod = 'IT001E04533765'  -- POD di test per PNO2G
  AND tipo_flusso_dx = 'PNO2G'
ORDER BY data_misura_dx DESC;

-- 3. VERIFICA POPOLAMENTO PIVA_DISTR E RAG_SOC_DISTR
SELECT
  pod,
  piva_distr,
  rag_soc_distr,
  anno,
  mese,
  executionid
FROM eng_test.erp_daily_no
WHERE executionid = 1762270210426
  AND anno = 2025
  AND mese = 9
  AND pod = 'IT001E04533765'  -- POD di test
  AND (piva_distr IS NOT NULL OR rag_soc_distr IS NOT NULL)
ORDER BY pod;

-- 4. VERIFICA RIPROPORZIONAMENTO CONSUMI (valori Q1-Q100 ~0.028)
SELECT
  pod,
  giorno,
  q1, q2, q3, q4, q5, q6, q7, q8, q9, q10,
  anno,
  mese,
  executionid
FROM eng_test.erp_daily_no
WHERE executionid = 1762270210426
  AND anno = 2025
  AND mese = 9
  AND pod = 'IT001E04280715'  -- POD di test per consumi
  AND giorno = 1
ORDER BY giorno;

-- 5. VERIFICA ESCLUSIONE SMIS_MN + SMIS_MN (non dovrebbero esserci record)
SELECT
  pod,
  tipo_flusso_sx,
  tipo_flusso_dx,
  stato,
  COUNT(*) as num_segmenti
FROM eng_test.erp_consumption_no
WHERE executionid = 1762270210426
  AND anno = 2025
  AND mese = 9
  AND tipo_flusso_sx = 'SMIS_MN'
  AND tipo_flusso_dx = 'SMIS_MN'
GROUP BY pod, tipo_flusso_sx, tipo_flusso_dx, stato;

-- 6. CONTEGGIO TOTALE RECORD PER TABELLA
SELECT 'erp_validated_mis_no' as tabella, COUNT(*) as totale
FROM eng_test.erp_validated_mis_no
WHERE executionid = 1762270210426 AND anno = 2025 AND mese = 9
UNION ALL
SELECT 'erp_consumption_no' as tabella, COUNT(*) as totale
FROM eng_test.erp_consumption_no
WHERE executionid = 1762270210426 AND anno = 2025 AND mese = 9
UNION ALL
SELECT 'erp_daily_no' as tabella, COUNT(*) as totale
FROM eng_test.erp_daily_no
WHERE executionid = 1762270210426 AND anno = 2025 AND mese = 9
UNION ALL
SELECT 'erp_aggregato_no' as tabella, COUNT(*) as totale
FROM eng_test.erp_aggregato_no
WHERE executionid = 1762270210426 AND anno = 2025 AND mese = 9;