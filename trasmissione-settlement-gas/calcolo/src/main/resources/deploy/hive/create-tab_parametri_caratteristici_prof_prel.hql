create external table ${tsg.db}.${tsg.TabParametriCaratteristiciProfPrel.tableName}
(
      beta1_prof double,
      beta2_prof double,
      beta3_prof double,
      beta4_prof double,
      cat_uso string,
      classe_prel string,
      prof string
) partitioned by (zona_clim string)
stored as parquet
location '${tsg.TabParametriCaratteristiciProfPrel.basepath}'