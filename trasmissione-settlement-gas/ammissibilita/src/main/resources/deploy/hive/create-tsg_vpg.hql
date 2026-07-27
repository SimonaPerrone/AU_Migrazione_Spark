create external table ${tsg.db}.${tsg.TSGVPG.tableName}
(
      n_id_tsg2_file bigint,
      giorno_riferimento string,
      c1_a1 string,
      c1_b1 string,
      c1_c1 string,
      c1_d1 string,
      c1_e1 string,
      c1_f1 string,
      c1_a2 string,
      c1_b2 string,
      c1_c2 string,
      c1_d2 string,
      c1_e2 string,
      c1_f2 string,
      c1_a3 string,
      c1_b3 string,
      c1_c3 string,
      c1_d3 string,
      c1_e3 string,
      c1_f3 string,
      c2 string,
      c4 string,
      t1_1 string,
      t1_2 string,
      t1_3 string,
      verifica_amm boolean,
      cod_causale string,
      motivazione string,
      nome_file string
) partitioned by (annotermico string, annomese string, executionid bigint, progressivo string)
stored as parquet
location '${tsg.TSGVPG.basepath}'