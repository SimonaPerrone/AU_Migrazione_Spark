package it.sferanet.au.controller.exclusionpdr

import it.sferanet.au.model.Flow
import it.sferanet.au.schema.{PdrMassivoSchema, PdrToExcludeSchema}
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ExclusionPdrController {

  def exclude(measures: RDD[Flow], pdrMassivo: DataFrame): (RDD[Flow], DataFrame) = {
    val filePath: String = Environment.getExclusionFilterCsvPath

    val exclusionPdr: DataFrame = Environment.getSqlContext
      .read
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .format("csv")
      .schema(PdrToExcludeSchema.createSparkSchema())
      .load(filePath)
      .select(PdrToExcludeSchema.pdr)
      .cache()

    val exclusionMassivo = pdrMassivo.join(broadcast(exclusionPdr), pdrMassivo(PdrMassivoSchema.codice_pdr) === exclusionPdr(PdrToExcludeSchema.pdr), "left")
      .filter(exclusionPdr(PdrToExcludeSchema.pdr).isNull)
      .drop(exclusionPdr(PdrToExcludeSchema.pdr))

    val rddExclusionPdr = Environment.getSparkContext.broadcast(exclusionPdr
      .rdd.map(row => row.getAs[String](PdrToExcludeSchema.pdr)).collect())

    val measureFilter = measures
      .keyBy(_.pdr)
      .filter({ case (pdr, _) => !rddExclusionPdr.value.contains(pdr) })
      .values

    (measureFilter, exclusionMassivo)
  }

}
