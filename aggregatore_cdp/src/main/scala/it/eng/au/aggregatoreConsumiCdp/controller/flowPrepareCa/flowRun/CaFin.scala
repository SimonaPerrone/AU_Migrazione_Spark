package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.flowRun

import it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.FlowCdpDatiPrelievoGas
import it.eng.au.aggregatoreConsumiCdp.schema.{CaFinalSchema, OutputHiveSchema}
import it.eng.au.aggregatoreConsumiCdp.utility.Constants.{DATA_DECORRENZA_FORMAT, TIMESTAMP_FORMAT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CaFin extends FlowCdpDatiPrelievoGas {
  override def addPiva(caFinal: DataFrame, distributore: DataFrame, azienda: DataFrame, udb: DataFrame): DataFrame = {
    caFinal
  }

  override def specificTransform(caFinal: DataFrame): DataFrame = {
    caFinal
      .withColumn(OutputHiveSchema.udd_oggetto_swithcing, lit(""))
      .withColumn(OutputHiveSchema.data_decorrenza, from_unixtime(unix_timestamp(concat(lit("01/10/"), col(CaFinalSchema.anno_competenza) - 1), DATA_DECORRENZA_FORMAT), TIMESTAMP_FORMAT))
      .selectExpr(OutputHiveSchema.getValues: _*)
  }
}