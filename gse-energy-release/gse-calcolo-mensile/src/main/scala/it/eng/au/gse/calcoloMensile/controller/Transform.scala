package it.eng.au.gse.calcoloMensile.controller

import it.eng.au.gse.calcoloMensile.schema.gse.{GseAggrMSchema, GseRichiestaSchema}
import it.eng.au.gse.common.dao.GsePerimetroDao
import it.eng.au.gse.common.schema.dwh.{DwhConsumiOutputSchema, DwhConsumiSchema}
import it.eng.au.gse.common.schema.gse.GsePerimetroSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, lit, round}
import org.apache.spark.sql.types.{DecimalType, LongType, TimestampType}
import org.apache.spark.storage.StorageLevel

object Transform {
  def joinPerimeterAndRequests(podPerimeter: DataFrame, newRequests: DataFrame): (DataFrame, DataFrame) = {
    val requests = podPerimeter
      .join(newRequests,
        newRequests(GseRichiestaSchema.t_mese_anno) === podPerimeter(GsePerimetroSchema.t_mese_anno) &&
        (newRequests(GseRichiestaSchema.t_cod_pod).isNull || podPerimeter(GsePerimetroSchema.t_cod_pod) === newRequests(GseRichiestaSchema.t_cod_pod)) &&
          (newRequests(GseRichiestaSchema.t_cliente).isNull || podPerimeter(GsePerimetroDao.t_cliente) === newRequests(GseRichiestaSchema.t_cliente)))
      .select(
        newRequests(GseRichiestaSchema.n_id_gse_richiesta_er_m),
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
      .withColumn(GseAggrMSchema.n_consumo_mensile, round(col(DwhConsumiOutputSchema.consumo), 3).cast(DecimalType(12,3)))
      .withColumn(GseAggrMSchema.d_data_creazione, lit(startDate).cast(TimestampType))
      .withColumn(GseAggrMSchema.n_execution_id, lit(executionId).cast(LongType))
      .selectExpr(GseAggrMSchema.getValues: _*)
  }
}
