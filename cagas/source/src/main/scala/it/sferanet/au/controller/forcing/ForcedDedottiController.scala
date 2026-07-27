package it.sferanet.au.controller.forcing

import it.sferanet.au.filterPdr.input.schema.ForcingDedottiSchema
import it.sferanet.au.filterPdr.input.struct.ForcingDedottiStruct
import it.sferanet.au.schema.PdrMassivoSchema
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, when}

object ForcedDedottiController {
  /**
   * Forza un determinato insieme di PdR come PdR dedotti (per un PdR dedotto non viene effettuato il calcolo della ca).
   */
  def forcing(pdrMassivo: DataFrame): DataFrame = {
    val filePath: String = Environment.getForcingDedottiCsvPath

    val forcingDedottiDF: DataFrame = Environment.getSqlContext.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .format("csv").schema(ForcingDedottiStruct.struct).load(filePath)

    pdrMassivo
      .join(broadcast(forcingDedottiDF), pdrMassivo(PdrMassivoSchema.codice_pdr) === forcingDedottiDF(ForcingDedottiSchema.pdr.toString), "left")
      .withColumn(PdrMassivoSchema.prelievo_annuo_prev_forced, when(col(ForcingDedottiSchema.pdr.toString).isNotNull, col(PdrMassivoSchema.n_prelievo_annuo)))
      .withColumn(PdrMassivoSchema.calcmode, when(col(ForcingDedottiSchema.pdr.toString).isNotNull, " con forzatura per valori anomali"))
      .drop(forcingDedottiDF.col(ForcingDedottiSchema.pdr.toString))
  }
}
