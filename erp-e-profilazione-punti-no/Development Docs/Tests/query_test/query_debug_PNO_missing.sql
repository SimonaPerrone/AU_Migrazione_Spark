-- Query di debug per capire perché il PNO non viene trovato
-- POD: IT001E04533765
-- Contesto: start_in = 20250831 (31 ago), stop_in = 20250930 (30 set)

-- ============================================================
-- 1) VERIFICA PRESENZA PNO nella tabella di input
--    Controlliamo se esistono righe PNO nella tabella flusso_misure_noaggr
-- ============================================================
SELECT 
  pod,
  tipo_flusso,
  data_misura,
  anno,
  mese,
  validato,
  tipodato_e,
  COUNT(*) as num_righe
FROM eng_test.flusso_misure_noaggr
WHERE pod = 'IT001E04533765'
  AND UPPER(TRIM(tipo_flusso)) IN ('PNO', 'PNO2G')
GROUP BY pod, tipo_flusso, data_misura, anno, mese, validato, tipodato_e
ORDER BY data_misura, tipo_flusso;

-- ============================================================
-- 2) VERIFICA FILTRI validato e tipodato_e 
--    Spark applica questi filtri prima di processare i dati
-- ============================================================
SELECT 
  pod,
  tipo_flusso,
  data_misura,
  anno,
  mese,
  validato,
  tipodato_e,
  CASE 
    WHEN validato = 'S' THEN 'OK_validato'
    ELSE 'FAIL_validato_non_S'
  END as check_validato,
  CASE 
    WHEN tipodato_e = '1' THEN 'OK_tipodato'
    ELSE 'FAIL_tipodato_non_1'
  END as check_tipodato,
  COUNT(*) as num_righe
FROM eng_test.flusso_misure_noaggr
WHERE pod = 'IT001E04533765'
  AND UPPER(TRIM(tipo_flusso)) IN ('PNO', 'PNO2G')
GROUP BY pod, tipo_flusso, data_misura, anno, mese, validato, tipodato_e
ORDER BY data_misura, tipo_flusso;

-- ============================================================
-- 3) SIMULAZIONE FILTRO DATA per SPONDA SINISTRA (start_in)
--    PNO/PNO2G in sponda sinistra devono avere data_misura = start_in
--    Per annomese=202509, start_in = 20250831
--    Il filtro Spark confronta: data_misura_yyyymmdd === start_in_column
-- ============================================================

-- Prima calcoliamo data_misura_yyyymmdd (come fa Spark)
-- e vediamo quali righe passerebbero il filtro start_in
WITH computed_dates AS (
  SELECT 
    pod,
    tipo_flusso,
    data_misura,
    anno,
    mese,
    -- Spark crea data_misura_yyyymmdd così:
    -- to_date(data_misura) -> date_format(..., 'yyyyMMdd')
    -- Simuliamo con Hive/Impala:
    CASE 
      WHEN LENGTH(TRIM(data_misura)) = 10 AND data_misura LIKE '____-__-__' 
        THEN regexp_replace(data_misura, '-', '')  -- da yyyy-MM-dd a yyyyMMdd
      WHEN LENGTH(TRIM(data_misura)) = 10 AND data_misura LIKE '__/__/____'
        THEN CONCAT(
          substring(data_misura, 7, 4),  -- anno
          substring(data_misura, 4, 2),  -- mese
          substring(data_misura, 1, 2)   -- giorno
        )
      ELSE data_misura  -- lascia così se già in formato corretto
    END as data_misura_yyyymmdd,
    validato,
    tipodato_e,
    '20250831' as start_in_atteso,
    '20250930' as stop_in_atteso
  FROM eng_test.flusso_misure_noaggr
  WHERE pod = 'IT001E04533765'
    AND UPPER(TRIM(tipo_flusso)) IN ('PNO', 'PNO2G')
    AND validato = 'S'
    AND tipodato_e = '1'
)
SELECT 
  pod,
  tipo_flusso,
  data_misura,
  data_misura_yyyymmdd,
  start_in_atteso,
  CASE 
    WHEN data_misura_yyyymmdd = start_in_atteso THEN 'PASS_sponda_SX'
    ELSE 'FAIL_sponda_SX'
  END as check_sponda_sx,
  CASE 
    WHEN data_misura_yyyymmdd = stop_in_atteso THEN 'PASS_sponda_DX'
    ELSE 'FAIL_sponda_DX'
  END as check_sponda_dx
FROM computed_dates
ORDER BY data_misura, tipo_flusso;

-- ============================================================
-- 4) VERIFICA OUTPUT erp_validated_mis_no
--    Controlliamo cosa è stato effettivamente scritto
-- ============================================================
SELECT 
  pod,
  tipo_flusso,
  data_misura,
  anno,
  mese,
  executionid
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND tipo_flusso IN ('PNO', 'PNO2G')
ORDER BY executionid DESC, data_misura, tipo_flusso
LIMIT 100;

-- ============================================================
-- 5) VERIFICA FORMATO DATA_MISURA
--    Controlliamo in che formato è memorizzata data_misura
-- ============================================================
SELECT DISTINCT
  data_misura,
  LENGTH(TRIM(data_misura)) as len,
  CASE 
    WHEN data_misura LIKE '____-__-__' THEN 'formato_yyyy-MM-dd'
    WHEN data_misura LIKE '__/__/____' THEN 'formato_dd/MM/yyyy'
    WHEN data_misura LIKE '________' THEN 'formato_yyyyMMdd'
    ELSE 'formato_sconosciuto'
  END as formato_rilevato
FROM eng_test.flusso_misure_noaggr
WHERE pod = 'IT001E04533765'
  AND UPPER(TRIM(tipo_flusso)) IN ('PNO', 'PNO2G')
ORDER BY data_misura;

-- ============================================================
-- 6) CONTA TOTALE PER TIPO_FLUSSO (per tutto il POD)
-- ============================================================
SELECT 
  tipo_flusso,
  COUNT(*) as totale_righe,
  COUNT(DISTINCT data_misura) as date_distinte,
  MIN(data_misura) as prima_data,
  MAX(data_misura) as ultima_data
FROM eng_test.flusso_misure_noaggr
WHERE pod = 'IT001E04533765'
GROUP BY tipo_flusso
ORDER BY tipo_flusso;
