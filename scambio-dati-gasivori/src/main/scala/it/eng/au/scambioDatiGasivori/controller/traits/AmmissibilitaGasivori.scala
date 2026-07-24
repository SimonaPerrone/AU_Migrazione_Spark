package it.eng.au.scambioDatiGasivori.controller.traits

import it.eng.au.scambioDatiGasivori.schema.gasivori.GasivoriPerimetroAmmSchema
import it.eng.au.scambioDatiGasivori.schema.output.PerimetroAmmOutputSchema
import it.eng.au.scambioDatiGasivori.schema.output.csv.PerimetroAmmCsvOutputSchema
import it.eng.au.scambioDatiGasivori.utility.{FileUtility, Properties}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, regexp_replace}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row}

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait AmmissibilitaGasivori extends RunnableAggregatorTrait {
  val cseaDest = "csea_dest"
  override val isAmmissibilita: Boolean = true
  override val operationName: String = "AMMISSIBILITA"
  override val csvFields: List[String] = PerimetroAmmCsvOutputSchema.getValues

  override def getInputTableFiltering: String = Properties.getAmmIdGasivoriFile

  val fileNameInColumn: String = PerimetroAmmOutputSchema.T_NOME_FILE_IN.toString

  override def writeFiles(rdd: RDD[(Map[String, String], Row)]): Unit = {
    val rddWithInfo = writeCsv(rdd, csvFields)
    writeInfoInTable(rddWithInfo)
  }

  override def getAggregato(): DataFrame = {
    val nIdGasivoriFile = Properties.getAmmIdGasivoriFile
    var df = inputDao.readTable
      .withColumn(cseaDest, lit("80198650584"))
      .where(col(GasivoriPerimetroAmmSchema.n_id_gasivori_file) === nIdGasivoriFile)
      .withColumn(GasivoriPerimetroAmmSchema.t_nome_file_in, regexp_replace(col(GasivoriPerimetroAmmSchema.t_nome_file_in), ".csv", ""))

    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      df = df.withColumnRenamed(dailyName, fileName)
    })

    df.selectExpr(aggregatoColumns.values.toList: _*)
  }

  def writeCsv(rddCsvOutput: RDD[(Map[String, String], Row)], columnsField: List[String]): RDD[(String, String, String, String, Timestamp, Long)] = {
    val csvOutputPath = getPathZipOutput
    val publicationType = getPublicationType
    val baseName = publicationType + baseNumber
    val daterun = Timestamp.valueOf(getDateToRun).toLocalDateTime
    val timestampToRun = Timestamp.valueOf(getDateToRun)
    val filteringValue = getInputTableFiltering
    val yearMonth = getYearMonth

    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>
      val countCsv = mapKeys(counterCsv)
      val fullPath = csvOutputPath + getCsvOutputPath(baseName, mapKeys, daterun, countCsv, yearMonth)
      val outputFolder = new File(fullPath).getParentFile

      val records = rows.toList.map(row => {
        columnsField.map(column => {
          row.getAs[String](column)
        }).mkString(CSV_SEPARATOR)
      })

      val header = if (writeCsvHeader)
        Some(columnsField.mkString(CSV_SEPARATOR))
      else None

      val exists = if (outputFolder.exists() && outputFolder.canWrite) {
        FileUtility.writeCsv(fullPath, header, records, appendMode = true)
        ""
      } else {
        logger.warn(s"Couldn't write to $outputFolder, the path does not exits.")
        s" Couldn't write to ${fullPath}, the path does not exits or permission are not set properly."
      }
      (filteringValue, operationName, destName, if (exists == "") fullPath else exists, timestampToRun, timestampToRun.getTime)
    })
  }

  def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, counterCsv: String, annoMese: String): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyField)
    val fileNameIn = mapKey(fileNameInColumn)

    s"/${baseName}_$pivaPathFolderHead/$year/$month/${fileNameIn}_AMM_${timestamp}.csv"
  }

  def getDistributedRDD(df: DataFrame, csvFields: List[String]): RDD[(Map[String, String], Row)] = {
    val keys = List(keyField) :+ counterCsv :+ fileNameInColumn

    val columns = (keys ++ csvFields).distinct

    var dfDistribution = distribution(df)
      .selectExpr(columns: _*)

    columns.foreach(column =>
      dfDistribution = dfDistribution.withColumn(column, col(column).cast(StringType))
    )

    dfDistribution
      .rdd
      .map(row => {
        val listMap = keys.map(column => column -> row.getAs[String](column)).toMap
        (listMap, row)
      })
  }
}