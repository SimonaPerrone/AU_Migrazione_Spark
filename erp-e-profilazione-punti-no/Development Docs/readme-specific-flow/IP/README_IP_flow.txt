IP Flow Quick Reference
=======================

1. HDFS Property File Management
--------------------------------
- Backup before editing:
  ```
  hdfs dfs -cp /user/eng_test/ERP/dev/deploy/conf/job.properties \
             /user/eng_test/ERP/dev/deploy/conf/job.properties.bak_$(date +%Y%m%d%H%M%S)
  ```
- Edit locally, then push:
  ```
  hdfs dfs -copyToLocal /user/eng_test/ERP/dev/deploy/conf/job.properties job.properties.tmp
  # Update Annomese (YYYYMM)
  sed -i 's/spark.app.energia_residuale_pariziale.paramentro.annomese=.*/spark.app.energia_residuale_pariziale.paramentro.annomese=202509/' job.properties.tmp
  # Update Area (empty = all)
  sed -i 's/spark.app.energia_residuale_pariziale.paramentro.area=.*/spark.app.energia_residuale_pariziale.paramentro.area=NORD/' job.properties.tmp
  # Update single distributor P.IVA (empty = all)
  sed -i 's/spark.app.energia_residuale_pariziale.paramentro.singola_piva_distributore=.*/spark.app.energia_residuale_pariziale.paramentro.singola_piva_distributore=05779711000/' job.properties.tmp
  hdfs dfs -put -f job.properties.tmp /user/eng_test/ERP/dev/deploy/conf/job.properties
  rm job.properties.tmp
  ```
- Verify:
  ```
  hdfs dfs -cat /user/eng_test/ERP/dev/deploy/conf/job.properties
  ```

2. Launching the IP Flow
------------------------
- Foreground run:
  ```
  bash ./spark-submit-erp.sh \
    --calcolo IP \
    --properties /user/eng_test/ERP/dev/deploy/conf/job.properties \
    --path_pod_esclusi /user/eng_test/ERP/dev/deploy/conf/pod_esclusi.txt
  ```
- Background with log:
  ```
  nohup bash ./spark-submit-erp.sh \
    --calcolo IP \
    --properties /user/eng_test/ERP/dev/deploy/conf/job.properties \
    --path_pod_esclusi /user/eng_test/ERP/dev/deploy/conf/pod_esclusi.txt \
    2>&1 | tee /tmp/spark-ip-$(date +%Y%m%d%H%M).log &
  ```

3. Input Data Checks
--------------------
- Distributors (RCU):
  ```
  SELECT n_id_azienda,
         t_piva,
         t_rag_soc,
         t_area_operativa
  FROM rcu.rcu_azienda_p
  ORDER BY n_id_azienda
  LIMIT 20;
  ```
- Aggregazione misure IP:
  ```
  SELECT t_pod,
         d_data,
         t_area,
         n_id_dis,
         annomese,
         h1_q1,
         h1_q2
  FROM au.v_aggregazione_misure_ip
  WHERE annomese = '202509'
    AND t_area   = 'NORD'
  ORDER BY d_data DESC
  LIMIT 20;
  ```
- Join rapido (area NORD, annomese 202509):
  ```
  SELECT ip.t_pod,
         ip.d_data,
         ip.t_area,
         ip.n_id_dis,
         az.t_piva,
         az.t_rag_soc,
         ip.h1_q1,
         ip.h1_q2
  FROM au.v_aggregazione_misure_ip ip
  JOIN rcu.rcu_azienda_p az
    ON ip.n_id_dis = az.n_id_azienda
  WHERE ip.annomese = '202509'
    AND ip.t_area   = 'NORD'
  LIMIT 20;
  ```

4. Output Validation
--------------------
- Partizioni e execution id:
  ```
  SHOW PARTITIONS eng_test.erp_aggregato_ip_o;
  SELECT executionid, COUNT(*) AS righe
  FROM eng_test.erp_aggregato_ip_o
  GROUP BY executionid
  ORDER BY executionid DESC;
  ```
- Righe per anno/mese/area:
  ```
  SELECT anno,
         mese,
         area,
         COUNT(*) AS righe
  FROM eng_test.erp_aggregato_ip_o
  GROUP BY anno, mese, area
  ORDER BY anno, mese, area;
  ```
- Filtra l'ultima partizione (sostituisci executionid):
  ```
  SELECT *
  FROM eng_test.erp_aggregato_ip_o
  WHERE executionid = '1762446603103'
    AND anno = 2025
    AND mese = 9
    AND area = 'NORD';
  ```
- Coerenza somma quartoraria (esempio P.IVA 05779711000):
  ```
  WITH sorgente AS (
    SELECT date_format(d_data, 'yyyy-MM-dd') AS giorno,
           t_area                             AS area,
           az.t_piva                          AS piva_distr,
           SUM(
             COALESCE(h1_q1 ,0) + COALESCE(h1_q2 ,0) + COALESCE(h1_q3 ,0) + COALESCE(h1_q4 ,0) +
             COALESCE(h2_q5 ,0) + COALESCE(h2_q6 ,0) + COALESCE(h2_q7 ,0) + COALESCE(h2_q8 ,0) +
             COALESCE(h3_q9 ,0) + COALESCE(h3_q10,0) + COALESCE(h3_q11,0) + COALESCE(h3_q12,0) +
             COALESCE(h4_q13,0) + COALESCE(h4_q14,0) + COALESCE(h4_q15,0) + COALESCE(h4_q16,0) +
             COALESCE(h5_q17,0) + COALESCE(h5_q18,0) + COALESCE(h5_q19,0) + COALESCE(h5_q20,0) +
             COALESCE(h6_q21,0) + COALESCE(h6_q22,0) + COALESCE(h6_q23,0) + COALESCE(h6_q24,0) +
             COALESCE(h7_q25,0) + COALESCE(h7_q26,0) + COALESCE(h7_q27,0) + COALESCE(h7_q28,0) +
             COALESCE(h8_q29,0) + COALESCE(h8_q30,0) + COALESCE(h8_q31,0) + COALESCE(h8_q32,0) +
             COALESCE(h9_q33,0) + COALESCE(h9_q34,0) + COALESCE(h9_q35,0) + COALESCE(h9_q36,0) +
             COALESCE(h10_q37,0) + COALESCE(h10_q38,0) + COALESCE(h10_q39,0) + COALESCE(h10_q40,0) +
             COALESCE(h11_q41,0) + COALESCE(h11_q42,0) + COALESCE(h11_q43,0) + COALESCE(h11_q44,0) +
             COALESCE(h12_q45,0) + COALESCE(h12_q46,0) + COALESCE(h12_q47,0) + COALESCE(h12_q48,0) +
             COALESCE(h13_q49,0) + COALESCE(h13_q50,0) + COALESCE(h13_q51,0) + COALESCE(h13_q52,0) +
             COALESCE(h14_q53,0) + COALESCE(h14_q54,0) + COALESCE(h14_q55,0) + COALESCE(h14_q56,0) +
             COALESCE(h15_q57,0) + COALESCE(h15_q58,0) + COALESCE(h15_q59,0) + COALESCE(h15_q60,0) +
             COALESCE(h16_q61,0) + COALESCE(h16_q62,0) + COALESCE(h16_q63,0) + COALESCE(h16_q64,0) +
             COALESCE(h17_q65,0) + COALESCE(h17_q66,0) + COALESCE(h17_q67,0) + COALESCE(h17_q68,0) +
             COALESCE(h18_q69,0) + COALESCE(h18_q70,0) + COALESCE(h18_q71,0) + COALESCE(h18_q72,0) +
             COALESCE(h19_q73,0) + COALESCE(h19_q74,0) + COALESCE(h19_q75,0) + COALESCE(h19_q76,0) +
             COALESCE(h20_q77,0) + COALESCE(h20_q78,0) + COALESCE(h20_q79,0) + COALESCE(h20_q80,0) +
             COALESCE(h21_q81,0) + COALESCE(h21_q82,0) + COALESCE(h21_q83,0) + COALESCE(h21_q84,0) +
             COALESCE(h22_q85,0) + COALESCE(h22_q86,0) + COALESCE(h22_q87,0) + COALESCE(h22_q88,0) +
             COALESCE(h23_q89,0) + COALESCE(h23_q90,0) + COALESCE(h23_q91,0) + COALESCE(h23_q92,0) +
             COALESCE(h24_q93,0) + COALESCE(h24_q94,0) + COALESCE(h24_q95,0) + COALESCE(h24_q96,0) +
             COALESCE(h25_q97,0) + COALESCE(h25_q98,0) + COALESCE(h25_q99,0) + COALESCE(h25_q100,0)
           ) AS energia_src
    FROM au.v_aggregazione_misure_ip ip
    JOIN rcu.rcu_azienda_p az
      ON ip.n_id_dis = az.n_id_azienda
    WHERE ip.annomese = '202509'
      AND ip.t_area   = 'NORD'
      AND az.t_piva   = '05779711000'
    GROUP BY date_format(d_data, 'yyyy-MM-dd'), t_area, az.t_piva
  ),
  target AS (
    SELECT concat_ws('-',
                     CAST(anno AS string),
                     lpad(CAST(mese AS string),2,'0'),
                     lpad(CAST(giorno AS string),2,'0')) AS giorno,
           area,
           piva_distr,
           SUM(
             COALESCE(q1 ,0) + COALESCE(q2 ,0) + COALESCE(q3 ,0) + COALESCE(q4 ,0) +
             COALESCE(q5 ,0) + COALESCE(q6 ,0) + COALESCE(q7 ,0) + COALESCE(q8 ,0) +
             COALESCE(q9 ,0) + COALESCE(q10,0) + COALESCE(q11,0) + COALESCE(q12,0) +
             COALESCE(q13,0) + COALESCE(q14,0) + COALESCE(q15,0) + COALESCE(q16,0) +
             COALESCE(q17,0) + COALESCE(q18,0) + COALESCE(q19,0) + COALESCE(q20,0) +
             COALESCE(q21,0) + COALESCE(q22,0) + COALESCE(q23,0) + COALESCE(q24,0) +
             COALESCE(q25,0) + COALESCE(q26,0) + COALESCE(q27,0) + COALESCE(q28,0) +
             COALESCE(q29,0) + COALESCE(q30,0) + COALESCE(q31,0) + COALESCE(q32,0) +
             COALESCE(q33,0) + COALESCE(q34,0) + COALESCE(q35,0) + COALESCE(q36,0) +
             COALESCE(q37,0) + COALESCE(q38,0) + COALESCE(q39,0) + COALESCE(q40,0) +
             COALESCE(q41,0) + COALESCE(q42,0) + COALESCE(q43,0) + COALESCE(q44,0) +
             COALESCE(q45,0) + COALESCE(q46,0) + COALESCE(q47,0) + COALESCE(q48,0) +
             COALESCE(q49,0) + COALESCE(q50,0) + COALESCE(q51,0) + COALESCE(q52,0) +
             COALESCE(q53,0) + COALESCE(q54,0) + COALESCE(q55,0) + COALESCE(q56,0) +
             COALESCE(q57,0) + COALESCE(q58,0) + COALESCE(q59,0) + COALESCE(q60,0) +
             COALESCE(q61,0) + COALESCE(q62,0) + COALESCE(q63,0) + COALESCE(q64,0) +
             COALESCE(q65,0) + COALESCE(q66,0) + COALESCE(q67,0) + COALESCE(q68,0) +
             COALESCE(q69,0) + COALESCE(q70,0) + COALESCE(q71,0) + COALESCE(q72,0) +
             COALESCE(q73,0) + COALESCE(q74,0) + COALESCE(q75,0) + COALESCE(q76,0) +
             COALESCE(q77,0) + COALESCE(q78,0) + COALESCE(q79,0) + COALESCE(q80,0) +
             COALESCE(q81,0) + COALESCE(q82,0) + COALESCE(q83,0) + COALESCE(q84,0) +
             COALESCE(q85,0) + COALESCE(q86,0) + COALESCE(q87,0) + COALESCE(q88,0) +
             COALESCE(q89,0) + COALESCE(q90,0) + COALESCE(q91,0) + COALESCE(q92,0) +
             COALESCE(q93,0) + COALESCE(q94,0) + COALESCE(q95,0) + COALESCE(q96,0) +
             COALESCE(q97,0) + COALESCE(q98,0) + COALESCE(q99,0) + COALESCE(q100,0)
           ) AS energia_tgt
    FROM eng_test.erp_aggregato_ip_o
    WHERE executionid = '1762446603103'
      AND anno        = 2025
      AND mese        = 9
      AND area        = 'NORD'
      AND piva_distr  = '05779711000'
    GROUP BY concat_ws('-',
                       CAST(anno AS string),
                       lpad(CAST(mese AS string),2,'0'),
                       lpad(CAST(giorno AS string),2,'0')),
             area,
             piva_distr
  )
  SELECT s.area,
         s.giorno,
         s.piva_distr,
         ABS(s.energia_src - COALESCE(t.energia_tgt,0)) AS scarto
  FROM sorgente s
  LEFT JOIN target t
    ON t.giorno     = s.giorno
   AND t.area       = s.area
   AND t.piva_distr = s.piva_distr
  WHERE ABS(s.energia_src - COALESCE(t.energia_tgt,0)) > 1e-6;
  ```
  *Output vuoto = aggregazione coerente con la view sorgente (entro la tolleranza 1e-6).*
