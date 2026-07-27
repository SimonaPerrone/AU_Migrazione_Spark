package it.eng.au.aggregatoreConsumiSbg.controller.traits

import it.eng.au.aggregatoreConsumiCommon.controller.traits.IncoerentiTrait
import it.eng.au.aggregatoreConsumiCommon.schema.IncoerentiOutputSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Implementa il processo di pubblicazione A+B per il processo SBG. Qui, a differenza di AGG, sono presenti i file CONF/NOCONF:
 * gli utenti segnalano tramite l'utilizzo di due liste (lista C e lista D) i PdR da inserire rispettivamente nel perimetro CONF e nel perimetro NOCONF.
 * Il processo deve creare, oltre all'usuale file di incoerenti, anche i file CONF/NOCONF e inserirli all'interno dello stesso zip.
 */
trait IncoerentiTraitSbg extends IncoerentiTrait {
  val incoerentiConfNoConf: IncoerentiConfNoConfTraitSbg

    /** [AU-603] Per ora non ci hanno richiesto di implementare il filtro anche per SBG */
    override def specificFilterForIncoerentiGdm: Column = lit(true)

  override def run(df: DataFrame): Unit = {
    if (isPdrListaCEnabled || isPdrListaDEnabled) df.persist(StorageLevel.MEMORY_AND_DISK)

    // 1. Creazione del DataFrame contenente i record anomali
    val aggregato = getAggregato(df)
    val aggregatoFiltered = aggregato.filter(keyFields.map(f => col(f).isNotNull).reduce(_ && _))
    val aggregatoForCsv = convertColumnsToString(aggregatoFiltered).na.fill("")

    // 2. Scrittura dataframe anomali nella directory tmp
    val csvOutputModel = getCsvOutputModel(aggregatoForCsv, csvFields)
    val rddCsvPathAggregato = writeCsv(csvOutputModel, csvFields)

    // 3. Lettura e scrittura anomali lista C nella directory tmp
    val rddCsvPathListaC = if (isPdrListaCEnabled) {
      val aggregatoC = incoerentiConfNoConf.getAggregato(df, getPdrListaCCsvPath)
      val csvOutputModelC = getCsvOutputModel(aggregatoC, csvFields)
      writeCsv(csvOutputModelC, incoerentiConfNoConf.csvFields, Some("CONF"))
    }
      else Environment.spark.sparkContext.emptyRDD: RDD[(String, String)]

    // 4. Lettura e scrittura anomali lista D nella directory tmp
    val rddCsvPathListaD = if (isPdrListaDEnabled) {
      val aggregatoD = incoerentiConfNoConf.getAggregato(df, getPdrListaDCsvPath)
      val csvOutputModelD = getCsvOutputModel(aggregatoD, csvFields)
      writeCsv(csvOutputModelD, incoerentiConfNoConf.csvFields, Some("NOCONF"))
    }
      else Environment.spark.sparkContext.emptyRDD: RDD[(String, String)]

    // 5. Unione degli RDD in modo tale da forzare la creazione di rddCsvPathListaC e rddCsvPathListaD
    val rddCsvPath = if (isPdrListaCEnabled || isPdrListaDEnabled) {
        rddCsvPathAggregato.union(rddCsvPathListaC).union(rddCsvPathListaD)
          .coalesce(rddCsvPathAggregato.getNumPartitions)
          .groupByKey()
          .mapValues(values => values.head)
    } else rddCsvPathAggregato

    // 6. Creazione dello zip
    val rddWithInfo = writeZip(rddCsvPath)

    // 7. Scrittura in tabella
    writeInfoInTable(rddWithInfo)
  }

  override def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, publicationType: String, sessionName: String, counterCsv: String, annoMese: String, optionalOperationName: Option[String] = None): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyFields.head)
    val pivaNameFile = keyFields.map(key => mapKey(key)).mkString("_")

    val operationNameString = optionalOperationName.getOrElse(operationName)

    //es SBG1_0123456789/2022/04/0123456789_SBG_INCOERENTI_202204_20220428105421_1.csv
    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_${publicationType}_${operationNameString}_${annoMese}_${timestamp}_${counterCsv}.csv"
  }

  override def getZipOutputName(pivaFolder: String, publicationType: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_DETTAGLIO_PDR_SBG_INCOERENTI_202204_20220428105421_1.zip
    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${publicationType}_${operationName}_${year}_${timestamp}_1.zip"
    zipName
  }
}