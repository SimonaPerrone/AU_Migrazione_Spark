package it.eng.au.gse.common.controller

import it.eng.au.gse.common.schema.dwh.{DwhConsumiOutputSchema, DwhConsumiSchema}
import it.eng.au.gse.common.schema.gse.GsePerimetroSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col}

object TransformCommon {
  def joinDwhConsumiWithPods(dwhConsumi: DataFrame, pods: DataFrame): DataFrame = {
    dwhConsumi
      .join(broadcast(pods),
        dwhConsumi(DwhConsumiSchema.pod14) === pods(GsePerimetroSchema.t_cod_pod) &&
          dwhConsumi(DwhConsumiOutputSchema.meseanno) === pods(GsePerimetroSchema.t_mese_anno))
      .drop(col(GsePerimetroSchema.t_cod_pod))
      .drop(col(GsePerimetroSchema.t_mese_anno))
  }
}
