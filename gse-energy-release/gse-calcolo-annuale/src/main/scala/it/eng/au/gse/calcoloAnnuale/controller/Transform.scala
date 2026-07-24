package it.eng.au.gse.calcoloAnnuale.controller

import it.eng.au.gse.calcoloAnnuale.schema.{GseAggrASchema, GseRichiestaSchema}
import it.eng.au.gse.common.dao.GsePerimetroDao
import it.eng.au.gse.common.schema.dwh.DwhConsumiOutputSchema
import it.eng.au.gse.common.schema.gse.GsePerimetroSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, LongType, TimestampType}
import org.apache.spark.storage.StorageLevel

object Transform {
  def joinPerimeterAndRequests(podPerimeter: DataFrame, newRequests: DataFrame): (DataFrame, DataFrame) = {
    val requests = podPerimeter
      .join(newRequests,
        newRequests(GseRichiestaSchema.t_anno) === podPerimeter(GsePerimetroDao.t_anno))
      .select(
        newRequests(GseRichiestaSchema.n_id_gse_richiesta_er_a),
        podPerimeter(GsePerimetroSchema.t_cod_pod),
        podPerimeter(GsePerimetroSchema.t_mese_anno)
      )
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)

    val podToCompute = requests
      .select(
        GsePerimetroSchema.t_cod_pod,
        GsePerimetroSchema.t_mese_anno
      ).distinct()

    (requests, podToCompute)
  }

  def joinRequestsAndConsumptions(requests: DataFrame, consumptions: DataFrame, startDate: String, executionId: Long): DataFrame = {
    requests
      .join(consumptions,
        requests(GsePerimetroSchema.t_cod_pod) === consumptions(DwhConsumiOutputSchema.codice_pod) &&
          requests(GsePerimetroSchema.t_mese_anno) === consumptions(DwhConsumiOutputSchema.meseanno), "left")
      .drop(consumptions(DwhConsumiOutputSchema.codice_pod))
      .drop(consumptions(DwhConsumiOutputSchema.meseanno))
      .withColumn(GseAggrASchema.n_consumo_mensile, round(col(DwhConsumiOutputSchema.consumo), 3).cast(DecimalType(12,3)))
      .withColumn(GseAggrASchema.d_data_creazione, lit(startDate).cast(TimestampType))
      .withColumn(GseAggrASchema.n_execution_id, lit(executionId).cast(LongType))
      .withColumn(GseAggrASchema.t_anno, from_unixtime(unix_timestamp(col(GsePerimetroSchema.t_mese_anno), "mm/YYYY"), "YYYY"))
      .withColumn(GseAggrASchema.t_mese, from_unixtime(unix_timestamp(col(GsePerimetroSchema.t_mese_anno), "mm/YYYY"), "mm"))
      .selectExpr(GseAggrASchema.getValues: _*)
  }
}
