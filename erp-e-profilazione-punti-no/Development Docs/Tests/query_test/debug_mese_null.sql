-- Verifica se mese è NULL o vuoto dopo la scrittura
SELECT 
    'CHECK_MESE_NULL' as query,
    pod,
    tipo_flusso,
    data_misura,
    anno,
    mese,
    CASE 
        WHEN mese IS NULL THEN 'MESE_NULL ✗'
        WHEN mese = '' THEN 'MESE_EMPTY ✗'
        WHEN LENGTH(mese) = 1 THEN 'MESE_NO_LPAD ✗'
        WHEN LENGTH(mese) = 2 THEN 'MESE_OK ✓'
        ELSE 'MESE_UNKNOWN ✗'
    END as mese_status
FROM eng_test.erp_validated_mis_no
WHERE executionid = 1762343753057
  AND anno = 2025
LIMIT 20;

-- Conta record con mese NULL o empty
SELECT 
    'COUNT_MESE_ISSUES' as query,
    CASE 
        WHEN mese IS NULL THEN 'MESE_NULL'
        WHEN mese = '' THEN 'MESE_EMPTY'
        WHEN LENGTH(mese) = 1 THEN 'MESE_NO_LPAD'
        WHEN LENGTH(mese) = 2 THEN 'MESE_OK'
        ELSE 'MESE_UNKNOWN'
    END as mese_status,
    COUNT(*) as total_records
FROM eng_test.erp_validated_mis_no
WHERE executionid = 1762343753057
  AND anno = 2025
GROUP BY 
    CASE 
        WHEN mese IS NULL THEN 'MESE_NULL'
        WHEN mese = '' THEN 'MESE_EMPTY'
        WHEN LENGTH(mese) = 1 THEN 'MESE_NO_LPAD'
        WHEN LENGTH(mese) = 2 THEN 'MESE_OK'
        ELSE 'MESE_UNKNOWN'
    END;

-- Refresh table statistics
COMPUTE STATS eng_test.erp_validated_mis_no;
