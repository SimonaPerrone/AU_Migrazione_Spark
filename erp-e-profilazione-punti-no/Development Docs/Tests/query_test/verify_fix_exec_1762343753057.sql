-- ============================================
-- VERIFICA FIX PNO/PNO2G con executionid 1762347576401
-- ============================================

-- Query DEBUG: Vediamo cosa c'è per questo POD e executionid (SENZA FILTRI SU TIPO_FLUSSO)
SELECT 
    'DEBUG_ALL_RECORDS' as query_name,
    pod,
    tipo_flusso,
    data_misura,
    anno,
    mese,
    executionid
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762347576401';

-- Query DEBUG2: Vediamo tutti i tipo_flusso per questo POD (qualsiasi executionid recente)
SELECT 
    'DEBUG_ALL_FLUSSI_POD' as query_name,
    executionid,
    tipo_flusso,
    COUNT(*) as cnt
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
GROUP BY executionid, tipo_flusso
ORDER BY executionid DESC
LIMIT 20;

-- Query DEBUG3: Vediamo se esiste PNO per qualsiasi POD con questo executionid
SELECT 
    'DEBUG_PNO_ANY_POD' as query_name,
    pod,
    tipo_flusso,
    data_misura,
    anno,
    mese
FROM eng_test.erp_validated_mis_no
WHERE executionid = '1762347576401'
  AND tipo_flusso = 'PNO'
LIMIT 10;

-- Query 1: Verifica PNO in erp_validated_mis_no (deve esistere con data_misura='2025-08-31' e mese='08')
SELECT 
    'Q1_VALIDATED_MIS_NO_PNO' as query_name,
    pod,
    tipo_flusso,
    data_misura,
    anno,
    mese,
    executionid,
    CASE 
        WHEN data_misura = '2025-08-31' AND mese = '08' THEN 'OK ✓' 
        ELSE 'FAIL ✗ data o mese sbagliato' 
    END as validation_status
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND tipo_flusso = 'PNO'
  AND executionid = '1762347576401'
  AND anno = '2025';

-- Query 2: Verifica PNO2G in erp_validated_mis_no (deve esistere con data_misura='2025-08-31' e mese='08')
SELECT 
    'Q2_VALIDATED_MIS_NO_PNO2G' as query_name,
    pod,
    tipo_flusso,
    data_misura,
    anno,
    mese,
    executionid,
    CASE 
        WHEN data_misura = '2025-08-31' AND mese = '08' THEN 'OK ✓' 
        ELSE 'FAIL ✗ data o mese sbagliato' 
    END as validation_status
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND tipo_flusso = 'PNO2G'
  AND executionid = '1762347576401'
  AND anno = '2025';

-- Query 3: Conta totale record PNO/PNO2G per il POD (devono essere 2: 1 PNO + 1 PNO2G)
SELECT 
    'Q3_COUNT_PNO_PNO2G' as query_name,
    tipo_flusso,
    COUNT(*) as total_count,
    CASE 
        WHEN COUNT(*) >= 1 THEN 'OK ✓' 
        ELSE 'FAIL ✗ missing records' 
    END as validation_status
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND tipo_flusso IN ('PNO', 'PNO2G')
  AND executionid = '1762347576401'
  AND anno = '2025'
GROUP BY tipo_flusso;

-- Query 4: Verifica formato mese (deve essere '08' con zero-padding, non '8')
SELECT 
    'Q4_MESE_FORMAT_CHECK' as query_name,
    pod,
    tipo_flusso,
    mese,
    LENGTH(mese) as mese_length,
    CASE 
        WHEN LENGTH(mese) = 2 AND mese = '08' THEN 'OK ✓ zero-padded' 
        WHEN LENGTH(mese) = 1 THEN 'FAIL ✗ missing zero-padding' 
        ELSE 'FAIL ✗ wrong format' 
    END as validation_status
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND tipo_flusso IN ('PNO', 'PNO2G')
  AND executionid = '1762347576401'
  AND anno = '2025';

-- Query 5: Verifica PNO in erp_consumption_no (propagazione a tabella successiva)
SELECT 
    'Q5_CONSUMPTION_NO_PNO' as query_name,
    pod,
    tipo_flusso,
    data_misura,
    anno,
    mese,
    executionid
FROM eng_test.erp_consumption_no
WHERE pod = 'IT001E04533765'
  AND tipo_flusso = 'PNO'
  AND executionid = '1762347576401'
  AND anno = '2025'
LIMIT 5;

-- Query 6: Verifica PNO in erp_daily_no (propagazione a tabella successiva)
SELECT 
    'Q6_DAILY_NO_PNO' as query_name,
    pod,
    data_misura,
    anno,
    mese,
    executionid
FROM eng_test.erp_daily_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762347576401'
  AND anno = '2025'
  AND mese = '08'
LIMIT 5;

-- Query 7: Verifica PNO in erp_aggregato_no (tabella finale)
SELECT 
    'Q7_AGGREGATO_NO_PNO' as query_name,
    pod,
    anno,
    mese,
    executionid,
    SUM(eam) as total_eam
FROM eng_test.erp_aggregato_no
WHERE pod = 'IT001E04533765'
  AND executionid = '1762347576401'
  AND anno = '2025'
  AND mese = '08'
GROUP BY pod, anno, mese, executionid;

-- Query 8: Confronto PRIMA/DOPO fix (controlla se ci sono record con executionid precedente)
SELECT 
    'Q8_COMPARISON' as query_name,
    executionid,
    tipo_flusso,
    data_misura,
    anno,
    mese,
    COUNT(*) as record_count
FROM eng_test.erp_validated_mis_no
WHERE pod = 'IT001E04533765'
  AND tipo_flusso IN ('PNO', 'PNO2G')
  AND anno = '2025'
GROUP BY executionid, tipo_flusso, data_misura, anno, mese
ORDER BY executionid DESC, tipo_flusso
LIMIT 20;
