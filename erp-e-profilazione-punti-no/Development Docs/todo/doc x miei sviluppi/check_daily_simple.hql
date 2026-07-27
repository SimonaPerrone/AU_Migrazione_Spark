-- Verifica semplice: vedi pod, anno, mese, giorno ultima esecuzione
SELECT 
  pod,
  anno,
  mese,
  giorno,
  executionid
FROM eng_test.erp_daily_no 
WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_daily_no)
LIMIT 100;
