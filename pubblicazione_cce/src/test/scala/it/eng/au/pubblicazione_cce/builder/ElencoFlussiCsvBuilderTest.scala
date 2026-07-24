package it.eng.au.pubblicazione_cce.builder

import it.eng.au.pubblicazione_cce.EnvironmentSparkTest
import it.eng.au.pubblicazione_cce.common.CostantiCCE._
import it.eng.au.pubblicazione_cce.mock.file.ElencoFlussiCsvBuilderMock
import it.eng.au.pubblicazione_cce.model.file.FileElencoFlussiModel
import it.eng.au.pubblicazione_cce.schema.file.FileSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.SparkSession
import org.junit.Assert

import java.sql.Timestamp
import java.time.LocalDate

class ElencoFlussiCsvBuilderTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /// DATI VALORIZZAZIONE TESTS ///

  val fileTimestamp = "20240101000000"
  val dataCalcolo = LocalDate.parse("2024-02-01")
  val annoCalcolo = "2024"
  val meseCalcolo = "02"
  val outputFilePath = "/"
  val maxLineCsv = 10

  val dataRichiesta = LocalDate.parse("2024-01-01")
  val dataMisure = Timestamp.valueOf("2024-01-31 00:00:00")
  val anno = "2024"
  val mese = "01"
  val piva = "piva1"
  val pod = "pod1"
  val pivaId = "pivaID1"
  val pivaUdd = "pivaUDD1"
  val ultimaDataAggiornamento = dataRichiesta.toString
  val executionid = "execid"
  val nomeFile = "file.xml"


  val elencoFlussiCsvBuilder = new ElencoFlussiCsvBuilderMock(
    fileTimestamp = fileTimestamp,
    dataCalcolo = dataCalcolo,
    outputFilePath = outputFilePath,
    maxLineCsv = maxLineCsv
  )

  /// TESTS

  def testElencoFlussiCsvBuilder(): Unit = {
    val flussi = Seq(FileElencoFlussiModel(
      piva = piva,
      ruolo = RUOLO_UDD,
      sessione = CCE1,
      processo = PROCESSO_P,
      annomese = anno + mese,
      timestamp = "2024-02-02 10:10:10",
      id_richiesta = "1",
      pod = pod,
      path_cloud = nomeFile,
      data_aggiornamento = ultimaDataAggiornamento
    )).toDS()

    val result = elencoFlussiCsvBuilder.computeCsvElements(flussi.toDF).head

    val expectedFileName = s"${piva}_${CCE}_${PROCESSO_P}_Elenco_Flussi_$anno${mese}_${fileTimestamp}_1_1.csv"
    val expectedPathRoot = s"/"
    val expectedPathSubDirectories = s"$CCE/$PATH_UDD/$piva/$PROCESSO_P/$annoCalcolo/$meseCalcolo/"
    val expectedFileFullName = expectedPathRoot + expectedPathSubDirectories + expectedFileName

    val expectedFileContent = s"${elencoFlussiCsvBuilder.headerCsv.getOrElse(List()).mkString(";")}\n" +
      s"$pod;$nomeFile;$anno$mese;$ultimaDataAggiornamento"

    Assert.assertEquals(expectedFileName, result.getAs[String](FileSchema.fileName))
    Assert.assertEquals(expectedFileContent, result.getAs[String](FileSchema.fileContent))
    Assert.assertEquals(expectedFileFullName, result.getAs[String](FileSchema.fileFullName))
    Assert.assertEquals(expectedPathRoot, result.getAs[String](FileSchema.filePathRoot))
    Assert.assertEquals(expectedPathSubDirectories, result.getAs[String](FileSchema.filePathSubDirectories))
  }

}
