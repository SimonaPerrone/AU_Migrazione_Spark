package it.eng.au.ccgPubblicazione.controller.runnablefile.traits.cdp

import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.RunnableAggregator
import it.eng.au.ccgPubblicazione.schema.cdp.output.{CdpConsumiOutputSchema, CdpFlussiOutputSchema}
import it.eng.au.ccgPubblicazione.schema.cdp.{CdpConsumptionRequestRunnableSchema, ValidatedFlowsCdpSchema}
import it.eng.au.ccgPubblicazione.utility.Constants.CDP
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.immutable.ListMap

trait CdpPdrElencoFlussi extends RunnableAggregator {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  override val operationName = CDP

  override val aggregatoColumnsConsumi: ListMap[String, String] = ListMap(
//    CdpConsumptionRequestRunnableSchema.anno.toString -> CdpConsumiOutputSchema.anno,
    CdpConsumptionRequestRunnableSchema.codice_pdr.toString -> CdpConsumiOutputSchema.cod_pdr,
    CdpConsumptionRequestRunnableSchema.codice_remi.toString -> CdpConsumiOutputSchema.cod_remi,
    CdpConsumptionRequestRunnableSchema.cat_uso.toString -> CdpConsumiOutputSchema.cat_uso,
    CdpConsumptionRequestRunnableSchema.classe_prelievo.toString -> CdpConsumiOutputSchema.classe_prelievo,
    CdpConsumptionRequestRunnableSchema.zona_climatica.toString -> CdpConsumiOutputSchema.zona_climatica,
    CdpConsumptionRequestRunnableSchema.id_reg_clim.toString -> CdpConsumiOutputSchema.id_reg_clim,
    CdpConsumptionRequestRunnableSchema.cod_prof_prel_std.toString -> CdpConsumiOutputSchema.cod_prof_prel_std,
    CdpConsumptionRequestRunnableSchema.prelievo_annuo_prev.toString -> CdpConsumiOutputSchema.prel_annuo_prev,
    CdpConsumptionRequestRunnableSchema.trattamento.toString -> CdpConsumiOutputSchema.trattamento,
    CdpConsumptionRequestRunnableSchema.tipo_trasmissione.toString -> CdpConsumiOutputSchema.sessione /*,
    CdpConsumptionRequestRunnableSchema.idRichiesta.toString -> CdpConsumiOutputSchema.id_richiesta.toString,
    CdpConsumptionRequestRunnableSchema.dataRichiesta.toString -> CdpConsumiOutputSchema.data_richiesta.toString*/
  )

  override val aggregatoColumnsFlussi: ListMap[String, String] = ListMap(
    CdpConsumptionRequestRunnableSchema.codice_pdr.toString -> CdpFlussiOutputSchema.pdr.toString,
    CdpConsumptionRequestRunnableSchema.prelievo_annuo_prev.toString -> CdpFlussiOutputSchema.prel_annuo_prev.toString,
    CdpConsumptionRequestRunnableSchema.trattamento.toString -> CdpFlussiOutputSchema.trattamento.toString,
    ValidatedFlowsCdpSchema.local_file.toString -> CdpFlussiOutputSchema.path_cloud.toString,
    CdpConsumptionRequestRunnableSchema.anno_competenza.toString -> CdpFlussiOutputSchema.AT.toString /*,
    CdpConsumptionRequestRunnableSchema.idRichiesta.toString -> CdpFlussiOutputSchema.id_richiesta.toString,
    CdpConsumptionRequestRunnableSchema.dataRichiesta.toString -> CdpFlussiOutputSchema.data_richiesta.toString*/
  )

  override val isAnno: Boolean = true
  override val mesiInAnno: String = ""
  override val headerCsvConsumi: List[String] = CdpConsumiOutputSchema.getValues
  override val headerCsvFlussi: List[String] = CdpFlussiOutputSchema.getValues
  override val pdrField: String = CdpConsumiOutputSchema.cod_pdr.toString
  override val dataField: String = CdpConsumiOutputSchema.anno.toString

  def consumptionFilter(df: DataFrame): DataFrame = {
    df
      .filter(

        fileSpecificFilterExpression
      )
  }

  override def getPdr(df: DataFrame): DataFrame = {
    var aggDF = consumptionFilter(df)
      .withColumn(CdpConsumiOutputSchema.anno, concat(lit("01/10/"), col(CdpConsumptionRequestRunnableSchema.anno_competenza)))

    aggregatoColumnsConsumi.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumn(fileName, col(dailyName).cast(StringType))
    })

    aggDF.filter(keyFieldsConsumi.values.map(f => col(f).isNotNull).reduce(_ && _))
      .selectExpr(headerCsvConsumi.union(keyFieldsConsumi.values.toSeq).distinct: _*).na.fill("")
  }

  override def getElencoFlussi(df: DataFrame, validate: DataFrame): DataFrame = {
    var aggDF = consumptionFilter(df)
      .join(validate, df(CdpConsumptionRequestRunnableSchema.codice_pdr) === validate(ValidatedFlowsCdpSchema.pdr), "left")

    aggregatoColumnsFlussi.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumn(fileName, col(dailyName).cast(StringType))
    })

    aggDF
      .filter(keyFieldsFlussi.values.map(f => col(f).isNotNull).reduce(_ && _))
      .selectExpr(headerCsvFlussi.union(keyFieldsFlussi.values.toSeq.union(List(keyFieldsFlussi(piva)))).distinct: _*)
      .na.fill("")
  }
}
