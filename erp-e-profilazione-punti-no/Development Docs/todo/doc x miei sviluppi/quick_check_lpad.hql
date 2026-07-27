-- =====================================================
-- QUICK CHECK: Verifica LPAD ultima esecuzione
-- =====================================================

USE eng_test;

-- Mostra tutti i valori di 'mese' distinti nell'ultima esecuzione
SELECT 
  mese,
  LENGTH(mese) AS len,
  COUNT(*) AS cnt,
  MIN(pod) AS esempio_pod
FROM eng_test.erp_validated_mis_no
WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_validated_mis_no)
GROUP BY mese
ORDER BY mese;

-- Verifica se ci sono mesi senza padding (len=1)
SELECT 
  CASE 
    WHEN MAX(LENGTH(mese)) = 2 AND MIN(LENGTH(mese)) = 2 THEN '✅ TUTTI I MESI HANNO PADDING CORRETTO'
    ELSE '❌ ATTENZIONE: Alcuni mesi non hanno padding!'
  END AS risultato
FROM eng_test.erp_validated_mis_no
WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_validated_mis_no);
