package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.traits.dettaglioFlussi

import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.RunnableAggregator
import it.eng.au.aggregatoreConsumiCdp.schema.{OutputCsvSchema, OutputHiveSchema, ValidatedFlowsSchema}
import it.eng.au.aggregatoreConsumiCdp.utility.Constants.CSV_SEPARATOR
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.functions.{col, regexp_extract}
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait DettaglioFlussi extends RunnableAggregator {
  // This value is used in getCsvOutputModel method
  override val annoTermicoColumnName: String = OutputCsvSchema.AT
  val secondaryPiva: String

  def fileSpecificFilterExpression: Column

  override def getAggregato(df: DataFrame): DataFrame = {
    val validatedFlows = getAndPrepareValidateFlow()
    getElencoFlussi(df, validatedFlows)
  }

  def getAndPrepareValidateFlow(): DataFrame = {
    Environment.sqlContext.table(Environment.getValidatedFlowTableName)
      .selectExpr(ValidatedFlowsSchema.getValues: _*)
      .filter(col(ValidatedFlowsSchema.executionid) === Environment.getCaFinalExecutionId)
      .drop(col(ValidatedFlowsSchema.executionid))
      .distinct()
  }

  def getElencoFlussi(df: DataFrame, validate: DataFrame): DataFrame = {
    val allColumns = (csvFields ++ keyFields :+ OutputCsvSchema.sessione.toString).distinct

    var aggDF = df
      .join(validate, df(OutputHiveSchema.cod_pdr) === validate(ValidatedFlowsSchema.pdr), "left")
      .drop(validate(ValidatedFlowsSchema.pdr))
      .withColumn(ValidatedFlowsSchema.local_file, regexp_extract(col(ValidatedFlowsSchema.local_file), "(\\/[0-9]{4}){2}\\/.*\\..*", 0))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumnRenamed(dailyName, fileName)
    })

    aggDF
      .filter(fileSpecificFilterExpression && obligatoryExpression)
      .selectExpr(allColumns: _*)
  }

  override def getCsvOutputPath(baseName: String, mapKeys: Map[String, String], date: LocalDateTime, sessione: String, annoCompetenza: String, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaMain = mapKeys(mainPiva)
    val pivaSecondary = mapKeys(secondaryPiva)

    s"/${baseName}_$pivaMain/$year/$month/${pivaMain}_${pivaSecondary}_CDP-${sessione}_Elenco_Flussi_${annoCompetenza}_${timestamp}_${counterCsv}.csv"
  }

  override def getZipOutputName(pivaFolder: String, sessione: String, annoCompetenza: String, today: LocalDateTime): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val zipName = s"/${pivaFolder}_CDP-${sessione}_Elenco_Flussi_${annoCompetenza}_${timestamp}_1.zip"
    zipName
  }

  override def getHeader(csvFields: List[String], mapKeys: Map[String, String], annoCompetenza: String, baseName: String): String = {
    csvFields.mkString(CSV_SEPARATOR)
  }
}
