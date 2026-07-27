package it.eng.au.ccgPubblicazione.controller.runnablefile.traits.aggsbg

import it.eng.au.ccgPubblicazione.schema.aggsbg.AggConsumptionRequestRunnableSchema
import it.eng.au.ccgPubblicazione.schema.aggsbg.output.AggConsumiEsclusiOutputSchema
import it.eng.au.ccgPubblicazione.utility.Constants.ESCLUSI
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, date_format, round, sum, to_date, trunc, udf, unix_timestamp}
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

trait AggSbgEsclusi extends AggSbgPdrElencoFlussi {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  override val headerCsvConsumi: List[String] = AggConsumiEsclusiOutputSchema.getValues

  val prelievoAggregato: String = "prelievo_aggregato"
  val forceExclusion = "force"

  val mappingCausaleInternalErrorCodePriority = Map(
    "1" -> "5"
    , "2" -> "9"
    , "3" -> "7"
    , "4" -> "8"
    , "5" -> "2"
    , "6" -> "3"
    , "8" -> "6"
    , "9" -> "4"
    , forceExclusion -> "1"
  )

  val mappingCausalePriorityExternalErrorCode = Map(
    "5" -> "T4"
    , "9" -> "T8"
    , "7" -> "T6"
    , "8" -> "T7"
    , "2" -> "T1"
    , "3" -> "M2"
    , "6" -> "T5"
    , "4" -> "T3"
    , "1" -> "Tf"
  )

  val udfMappingCausaleInternalErrorCodePriority: UserDefinedFunction = udf((errorCode: String) => mappingCausaleInternalErrorCodePriority(errorCode))

  val udfMappingCausalePriorityExternalErrorCode: UserDefinedFunction = udf((errorCode: String) => mappingCausalePriorityExternalErrorCode(errorCode))


  override val publicationType: String = ESCLUSI

  override val aggregatoColumnsConsumi: ListMap[String, String] = ListMap(
    AggConsumptionRequestRunnableSchema.pdr.toString -> AggConsumiEsclusiOutputSchema.cod_pdr.toString,
    AggConsumptionRequestRunnableSchema.pivaDistr.toString -> AggConsumiEsclusiOutputSchema.piva_distr.toString,
    AggConsumptionRequestRunnableSchema.pivaIt.toString -> AggConsumiEsclusiOutputSchema.piva_it.toString,
    AggConsumptionRequestRunnableSchema.pivaUdd.toString -> AggConsumiEsclusiOutputSchema.piva_udd.toString,
    AggConsumptionRequestRunnableSchema.pivaUdb.toString -> AggConsumiEsclusiOutputSchema.piva_udb.toString,
    AggConsumptionRequestRunnableSchema.dtg.toString -> AggConsumiEsclusiOutputSchema.dtg.toString,
    AggConsumptionRequestRunnableSchema.codRemi.toString -> AggConsumiEsclusiOutputSchema.cod_remi.toString,
    AggConsumptionRequestRunnableSchema.ca.toString -> AggConsumiEsclusiOutputSchema.prel_annuo_prev.toString,
    AggConsumptionRequestRunnableSchema.idRegClim.toString -> AggConsumiEsclusiOutputSchema.id_reg_clim.toString,
    AggConsumptionRequestRunnableSchema.codProfStd.toString -> AggConsumiEsclusiOutputSchema.cod_prof_prel_std.toString,
    AggConsumptionRequestRunnableSchema.treatment.toString -> AggConsumiEsclusiOutputSchema.trattamento.toString,
    AggConsumptionRequestRunnableSchema.tipoCliente.toString -> AggConsumiEsclusiOutputSchema.tipo_cliente.toString,
    AggConsumptionRequestRunnableSchema.causale.toString -> AggConsumiEsclusiOutputSchema.causale.toString,
    prelievoAggregato -> AggConsumiEsclusiOutputSchema.prelievo_aggregato.toString,
    AggConsumptionRequestRunnableSchema.session.toString -> AggConsumiEsclusiOutputSchema.sessione.toString,
    AggConsumptionRequestRunnableSchema.annoMese.toString -> AggConsumiEsclusiOutputSchema.annoMese.toString
  )

  /**
   * Esegue i filtri e le operazioni al fine di individuare i PdR esclusi a partire dal dataframe dei consumi [[df]].
   *
   * @param df dataframe dei consumi
   * @return [[df]] contenente soltanto i PdR esclusi
   */
  override def consumptionFilter(df: DataFrame): DataFrame = {
    getExcludedPdrs(df)
  }

  override def getPdr(df: DataFrame): DataFrame = {
    val columnsForGroupBy = (aggregatoColumnsConsumi.keySet.toSeq.union(keyFiledsPreRenamed.values.toSeq)).distinct.map(col)
    val windowForExcludedPdrs = Window.partitionBy(columnsForGroupBy.diff(List(col(prelievoAggregato))):_*)
    val dataColName = "date_trunc"

    var aggDF = consumptionFilter(df)
      .withColumn(prelievoAggregato, round(sum(col(AggConsumptionRequestRunnableSchema.value)).over(windowForExcludedPdrs)).cast(IntegerType))
      .withColumn(dataColName, trunc(col(AggConsumptionRequestRunnableSchema.date), "month"))
      .filter(col(AggConsumptionRequestRunnableSchema.date) === col(dataColName))
      .withColumn(AggConsumiEsclusiOutputSchema.data, date_format(trunc(to_date(unix_timestamp(col(AggConsumptionRequestRunnableSchema.annoMese), "yyyyMM").cast(TimestampType)), "month"), "dd/MM/yyyy"))
      .withColumn(AggConsumiEsclusiOutputSchema.causale, udfMappingCausaleInternalErrorCodePriority(col(AggConsumptionRequestRunnableSchema.errorCode)))
      .withColumn(AggConsumiEsclusiOutputSchema.causale, udfMappingCausalePriorityExternalErrorCode(col(AggConsumiEsclusiOutputSchema.causale)))
      .drop(dataColName)

    aggregatoColumnsConsumi.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumn(fileName, col(dailyName).cast(StringType))
    })

    aggDF.filter(keyFieldsConsumi.values.map(f => col(f).isNotNull).reduce(_ && _))
      .selectExpr(headerCsvConsumi.union(keyFieldsConsumi.values.toSeq).distinct: _*).na.fill("")
  }

  override def getCsvOutputPath(
                                 mapKey: Map[String, String]
                                 , date: LocalDateTime
                                 , counterCsv: String
                                 , annoMese: String
                                 , isElencoFlussi: Boolean
                               ): String = {
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaUtente = mapKey(piva)
    val idRichiesta = mapKey(idrichiesta)
    val elencoFlussiPath = if (isElencoFlussi) "_Elenco_Flussi" else ""
    val esclusiPubType = if (isElencoFlussi) "" else s"_Elenco_Esclusi"

    val outputPath = getOutputPath(pivaUtente, date)

    s"/${outputPath}/$idRichiesta/${pivaUtente}_${ccg}_${operationName}${elencoFlussiPath}${esclusiPubType}_${annoMese}_${timestamp}_${idRichiesta}_${counterCsv}.csv"
  }
}
