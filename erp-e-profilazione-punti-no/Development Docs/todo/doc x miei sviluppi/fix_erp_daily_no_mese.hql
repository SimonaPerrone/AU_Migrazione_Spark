-- =====================================================
-- RICOSTRUZIONE erp_daily_no con mese STRING
-- =====================================================
-- Problema: File Parquet vecchi hanno mese INT, 
--           ma vogliamo mese STRING con padding
-- Soluzione: Backup + Ricostruzione con LPAD
-- =====================================================

USE eng_test;

-- STEP 1: Rinomina tabella attuale come backup
ALTER TABLE eng_test.erp_daily_no RENAME TO eng_test.erp_daily_no_backup_old_int;

-- STEP 2: Crea nuova tabella con mese STRING
CREATE EXTERNAL TABLE eng_test.erp_daily_no (
  pod STRING,
  anno INT,
  mese STRING COMMENT 'Mese con zero-padding (es. 09)',
  giorno DATE,
  area STRING,
  piva_distr STRING,
  rag_soc_distr STRING,
  q1 DOUBLE, q2 DOUBLE, q3 DOUBLE, q4 DOUBLE, q5 DOUBLE,
  q6 DOUBLE, q7 DOUBLE, q8 DOUBLE, q9 DOUBLE, q10 DOUBLE,
  q11 DOUBLE, q12 DOUBLE, q13 DOUBLE, q14 DOUBLE, q15 DOUBLE,
  q16 DOUBLE, q17 DOUBLE, q18 DOUBLE, q19 DOUBLE, q20 DOUBLE,
  q21 DOUBLE, q22 DOUBLE, q23 DOUBLE, q24 DOUBLE, q25 DOUBLE,
  q26 DOUBLE, q27 DOUBLE, q28 DOUBLE, q29 DOUBLE, q30 DOUBLE,
  q31 DOUBLE, q32 DOUBLE, q33 DOUBLE, q34 DOUBLE, q35 DOUBLE,
  q36 DOUBLE, q37 DOUBLE, q38 DOUBLE, q39 DOUBLE, q40 DOUBLE,
  q41 DOUBLE, q42 DOUBLE, q43 DOUBLE, q44 DOUBLE, q45 DOUBLE,
  q46 DOUBLE, q47 DOUBLE, q48 DOUBLE, q49 DOUBLE, q50 DOUBLE,
  q51 DOUBLE, q52 DOUBLE, q53 DOUBLE, q54 DOUBLE, q55 DOUBLE,
  q56 DOUBLE, q57 DOUBLE, q58 DOUBLE, q59 DOUBLE, q60 DOUBLE,
  q61 DOUBLE, q62 DOUBLE, q63 DOUBLE, q64 DOUBLE, q65 DOUBLE,
  q66 DOUBLE, q67 DOUBLE, q68 DOUBLE, q69 DOUBLE, q70 DOUBLE,
  q71 DOUBLE, q72 DOUBLE, q73 DOUBLE, q74 DOUBLE, q75 DOUBLE,
  q76 DOUBLE, q77 DOUBLE, q78 DOUBLE, q79 DOUBLE, q80 DOUBLE,
  q81 DOUBLE, q82 DOUBLE, q83 DOUBLE, q84 DOUBLE, q85 DOUBLE,
  q86 DOUBLE, q87 DOUBLE, q88 DOUBLE, q89 DOUBLE, q90 DOUBLE,
  q91 DOUBLE, q92 DOUBLE, q93 DOUBLE, q94 DOUBLE, q95 DOUBLE,
  q96 DOUBLE, q97 DOUBLE, q98 DOUBLE, q99 DOUBLE, q100 DOUBLE,
  executionid STRING
)
STORED AS PARQUET
LOCATION 'hdfs://hdfscdp.fsisilon.siiau.local:8020/user/hive/warehouse/eng_test.db/erp_daily_no_new'
TBLPROPERTIES (
  'EXTERNAL'='TRUE',
  'external.table.purge'='TRUE'
);

-- STEP 3: Inserisci dati vecchi con LPAD su mese
INSERT OVERWRITE TABLE eng_test.erp_daily_no
SELECT 
  pod,
  anno,
  LPAD(CAST(mese AS STRING), 2, '0') AS mese,  -- Converte INT a STRING con padding
  giorno,
  area,
  piva_distr,
  rag_soc_distr,
  q1, q2, q3, q4, q5, q6, q7, q8, q9, q10,
  q11, q12, q13, q14, q15, q16, q17, q18, q19, q20,
  q21, q22, q23, q24, q25, q26, q27, q28, q29, q30,
  q31, q32, q33, q34, q35, q36, q37, q38, q39, q40,
  q41, q42, q43, q44, q45, q46, q47, q48, q49, q50,
  q51, q52, q53, q54, q55, q56, q57, q58, q59, q60,
  q61, q62, q63, q64, q65, q66, q67, q68, q69, q70,
  q71, q72, q73, q74, q75, q76, q77, q78, q79, q80,
  q81, q82, q83, q84, q85, q86, q87, q88, q89, q90,
  q91, q92, q93, q94, q95, q96, q97, q98, q99, q100,
  executionid
FROM eng_test.erp_daily_no_backup_old_int;

-- STEP 4: Verifica conteggio
SELECT COUNT(*) AS backup_count FROM eng_test.erp_daily_no_backup_old_int;
SELECT COUNT(*) AS new_count FROM eng_test.erp_daily_no;

-- STEP 5: Verifica valori mese (devono essere tutti length=2)
SELECT DISTINCT mese, LENGTH(mese) AS len 
FROM eng_test.erp_daily_no 
ORDER BY mese;
