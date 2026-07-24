package it.eng.au.gse.common.controller

import it.eng.au.gse.common.schema.dwh.{DwhConsumiOutputSchema, DwhConsumiSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, lpad}
import org.apache.spark.sql.types.StringType

object PrepareCommon {
  def prepareDwhConsumi(dwhConsumi: DataFrame, yearMonthList: List[(Int, Int)]): DataFrame = {
    val partitionPruningCondition = yearMonthList
      .map({ case (year, month) => col(DwhConsumiSchema.anno) === year && col(DwhConsumiSchema.mese) === month })
      .reduce(_ || _)

    dwhConsumi
      .where(partitionPruningCondition)
      .withColumn(DwhConsumiSchema.anno, col(DwhConsumiSchema.anno).cast(StringType))
      .withColumn(DwhConsumiSchema.mese, lpad(col(DwhConsumiSchema.mese).cast(StringType), 2, "0"))
      .withColumn(DwhConsumiOutputSchema.meseanno, concat_ws("/", col(DwhConsumiSchema.mese), col(DwhConsumiSchema.anno)))
  }
}
