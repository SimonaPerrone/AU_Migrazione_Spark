package it.eng.au.gse.common.controller

import it.eng.au.gse.common.schema.dwh.{DwhConsumiOutputSchema, DwhConsumiSchema}
import it.eng.au.gse.common.utility.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object ConsumptionController {
  def computeMonthlyConsumptions(dwhConsumi: DataFrame): DataFrame = {
    val rankCol = "rank_column"
    val windowByPodAnnoMese = Window.partitionBy(DwhConsumiSchema.pod14, DwhConsumiSchema.anno, DwhConsumiSchema.mese).orderBy(col(DwhConsumiSchema.versione).desc)

    val columnsToRound = List(
      DwhConsumiSchema.somma_eam,
      DwhConsumiSchema.somma_eaf1, DwhConsumiSchema.somma_eaf2, DwhConsumiSchema.somma_eaf3,
      DwhConsumiSchema.somma_eaf4, DwhConsumiSchema.somma_eaf5, DwhConsumiSchema.somma_eaf6)

    val columnsForConsumption = List(
      DwhConsumiSchema.e1, DwhConsumiSchema.e2, DwhConsumiSchema.e3, DwhConsumiSchema.e4, DwhConsumiSchema.e5,
      DwhConsumiSchema.e6, DwhConsumiSchema.e7, DwhConsumiSchema.e8, DwhConsumiSchema.e9, DwhConsumiSchema.e10,
      DwhConsumiSchema.e11, DwhConsumiSchema.e12, DwhConsumiSchema.e13, DwhConsumiSchema.e14, DwhConsumiSchema.e15,
      DwhConsumiSchema.e16, DwhConsumiSchema.e17, DwhConsumiSchema.e18, DwhConsumiSchema.e19, DwhConsumiSchema.e20,
      DwhConsumiSchema.e21, DwhConsumiSchema.e22, DwhConsumiSchema.e23, DwhConsumiSchema.e24, DwhConsumiSchema.e25,
      DwhConsumiSchema.e26, DwhConsumiSchema.e27, DwhConsumiSchema.e28, DwhConsumiSchema.e29, DwhConsumiSchema.e30,
      DwhConsumiSchema.e31, DwhConsumiSchema.e32, DwhConsumiSchema.e33, DwhConsumiSchema.e34, DwhConsumiSchema.e35,
      DwhConsumiSchema.e36, DwhConsumiSchema.e37, DwhConsumiSchema.e38, DwhConsumiSchema.e39, DwhConsumiSchema.e40,
      DwhConsumiSchema.e41, DwhConsumiSchema.e42, DwhConsumiSchema.e43, DwhConsumiSchema.e44, DwhConsumiSchema.e45,
      DwhConsumiSchema.e46, DwhConsumiSchema.e47, DwhConsumiSchema.e48, DwhConsumiSchema.e49, DwhConsumiSchema.e50,
      DwhConsumiSchema.e51, DwhConsumiSchema.e52, DwhConsumiSchema.e53, DwhConsumiSchema.e54, DwhConsumiSchema.e55,
      DwhConsumiSchema.e56, DwhConsumiSchema.e57, DwhConsumiSchema.e58, DwhConsumiSchema.e59, DwhConsumiSchema.e60,
      DwhConsumiSchema.e61, DwhConsumiSchema.e62, DwhConsumiSchema.e63, DwhConsumiSchema.e64, DwhConsumiSchema.e65,
      DwhConsumiSchema.e66, DwhConsumiSchema.e67, DwhConsumiSchema.e68, DwhConsumiSchema.e69, DwhConsumiSchema.e70,
      DwhConsumiSchema.e71, DwhConsumiSchema.e72, DwhConsumiSchema.e73, DwhConsumiSchema.e74, DwhConsumiSchema.e75,
      DwhConsumiSchema.e76, DwhConsumiSchema.e77, DwhConsumiSchema.e78, DwhConsumiSchema.e79, DwhConsumiSchema.e80,
      DwhConsumiSchema.e81, DwhConsumiSchema.e82, DwhConsumiSchema.e83, DwhConsumiSchema.e84, DwhConsumiSchema.e85,
      DwhConsumiSchema.e86, DwhConsumiSchema.e87, DwhConsumiSchema.e88, DwhConsumiSchema.e89, DwhConsumiSchema.e90,
      DwhConsumiSchema.e91, DwhConsumiSchema.e92, DwhConsumiSchema.e93, DwhConsumiSchema.e94, DwhConsumiSchema.e95,
      DwhConsumiSchema.e96, DwhConsumiSchema.e97, DwhConsumiSchema.e98, DwhConsumiSchema.e99, DwhConsumiSchema.e100
    )

    var df = dwhConsumi
      .withColumn(rankCol, rank().over(windowByPodAnnoMese))
      .where(col(rankCol) === lit(1))
      .drop(rankCol)

    columnsToRound.foreach(colName =>
      df = df.withColumn(colName, greatest(round(col(colName).cast(DoubleType), 3), lit(0.0)))
    )

    columnsForConsumption.foreach(colName =>
      df = df.withColumn(colName, coalesce(col(colName), lit(0.0)))
    )

    df
      .withColumn(DwhConsumiOutputSchema.consumo,
        when(col(DwhConsumiSchema.trattamento) === TRATTAMENTO_M, col(DwhConsumiSchema.somma_eam))
          .when(col(DwhConsumiSchema.trattamento).isInCollection(List(TRATTAMENTO_F, TRATTAMENTO_C)),
            col(DwhConsumiSchema.somma_eaf1) + col(DwhConsumiSchema.somma_eaf2) + col(DwhConsumiSchema.somma_eaf3) +
              col(DwhConsumiSchema.somma_eaf4) + col(DwhConsumiSchema.somma_eaf5) + col(DwhConsumiSchema.somma_eaf6)
          )
          .when(col(DwhConsumiSchema.trattamento) === TRATTAMENTO_O, columnsForConsumption.map(col(_)).reduce(_ + _))
          .otherwise(lit(null).cast(DoubleType))
      )
      .withColumnRenamed(DwhConsumiSchema.pod14, DwhConsumiOutputSchema.codice_pod)
      .groupBy(
        DwhConsumiOutputSchema.codice_pod,
        DwhConsumiOutputSchema.meseanno
      )
      .agg(sum(DwhConsumiOutputSchema.consumo).as(DwhConsumiOutputSchema.consumo))
      .selectExpr(DwhConsumiOutputSchema.getValues: _*)
  }
}
