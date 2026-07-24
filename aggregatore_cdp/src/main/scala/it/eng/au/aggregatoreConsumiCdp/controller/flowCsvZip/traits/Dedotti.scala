package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.traits

import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.RunnableAggregator
import it.eng.au.aggregatoreConsumiCdp.schema.{OutputCsvSchema, OutputHiveSchema}
import it.eng.au.aggregatoreConsumiCdp.utility.Constants.{CSV_SEPARATOR, DATA_DECORRENZA_FORMAT, TIMESTAMP_FORMAT}
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait Dedotti extends RunnableAggregator {
  val operationName = "DEDOTTI"
  override val annoTermicoColumnName: String = OutputCsvSchema.anno

  def fileSpecificFilterExpression: Column

  override def getAggregato(df: DataFrame): DataFrame = {
    var aggDF = df
    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumnRenamed(dailyName, fileName)
    })

    val aggFilter = aggDF
      .filter(fileSpecificFilterExpression && obligatoryExpression)
      .withColumn(OutputHiveSchema.data_decorrenza, from_unixtime(unix_timestamp(col(OutputHiveSchema.data_decorrenza), TIMESTAMP_FORMAT), DATA_DECORRENZA_FORMAT))

    aggFilter
  }

  override def getZipOutputName(pivaFolder: String, sessione: String, annoCompetenza: String, today: LocalDateTime): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${baseName}-${sessione}_${operationName}_${annoCompetenza}_${timestamp}_1.zip"
    zipName
  }

  override def getCsvOutputPath(baseName: String, mapKeys: Map[String, String], date: LocalDateTime, sessione: String, annoCompetenza: String, counterCsv: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKeys(mainPiva)
    val pivaNameFile = keyFields.map(mapKeys(_)).mkString("_")

    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_CDP-${sessione}_${operationName}_${annoCompetenza}_${timestamp}_${counterCsv}.csv"
  }

  override def getHeader(csvFields: List[String], mapKeys: Map[String, String], annoCompetenza: String, baseName: String): String = {
    csvFields.mkString(CSV_SEPARATOR)
  }
}
