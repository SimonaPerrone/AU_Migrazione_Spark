-- Query semplice per vedere i dati di erp_daily_no ultima esecuzione
SELECT * 
FROM eng_test.erp_daily_no 
WHERE executionid = (SELECT MAX(executionid) FROM eng_test.erp_daily_no)
LIMIT 100;
