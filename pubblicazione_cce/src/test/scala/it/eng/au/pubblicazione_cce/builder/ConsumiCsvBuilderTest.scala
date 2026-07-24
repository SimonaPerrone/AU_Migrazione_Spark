package it.eng.au.pubblicazione_cce.builder

import it.eng.au.pubblicazione_cce.EnvironmentSparkTest
import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.common.CostantiCCE.{CCE, CCE1, PATH_ID, PROCESSO_P, RUOLO_DISTR, RUOLO_SII}
import it.eng.au.pubblicazione_cce.mock.file.ConsumiCsvBuilderMock
import it.eng.au.pubblicazione_cce.model.file.FileConsumiModel
import it.eng.au.pubblicazione_cce.schema.file.FileSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.SparkSession
import org.junit.Assert

import java.sql.Timestamp
import java.time.LocalDate

class ConsumiCsvBuilderTest extends EnvironmentSparkTest {
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


  val consumiCsvBuilder = new ConsumiCsvBuilderMock(
    fileTimestamp = fileTimestamp,
    dataCalcolo = dataCalcolo,
    outputFilePath = outputFilePath,
    maxLineCsv = maxLineCsv
  )

  /// TESTS

  def testConsumiCsvBuilder(): Unit = {
    val consumi = Seq(FileConsumiModel(
      piva = piva,
      sessione = CCE1,
      ruolo = CostantiCCE.RUOLO_DISTR,
      processo = PROCESSO_P,
      anno = anno,
      mese = mese,
      timestamp = "2024-02-02 10:10:10",
      id_richiesta = "1",
      data = dataMisure.toString.substring(0, 10),
      cod_pod = pod,
      piva_distr = pivaId,
      piva_udd = pivaUdd,
      h01 = "1.0", h02 = "2.0", h03 = "3.0", h04 = "3.0", h05 = "3.0", h06 = "3.0", h07 = "3.0", h08 = "0.0", h09 = "0.0",
      h10 = "0.0", h11 = "0.0", h12 = "0.0", h13 = "0.0", h14 = "0.0", h15 = "0.0", h16 = "0.0", h17 = "17.0", h18 = "0.0",
      h19 = "0.0", h20 = "0.0", h21 = "21.0", h22 = "0.0", h23 = "0.0", h24 = "0.0", h25 = "0.0",
      data_aggiornamento = ultimaDataAggiornamento,
      nome_file = nomeFile,
      executionid = executionid
    )).toDS()

    val result = consumiCsvBuilder.computeCsvElements(consumi.toDF).head

    val expectedFileName = s"${piva}_${CCE}_${PROCESSO_P}_$anno${mese}_${fileTimestamp}_1_1.csv"
    val expectedPathRoot = s"/"
    val expectedPathSubDirectories = s"$CCE/$PATH_ID/$piva/$PROCESSO_P/$annoCalcolo/$meseCalcolo/"
    val expectedFileFullName = expectedPathRoot + expectedPathSubDirectories + expectedFileName

    val expectedFileContent = s"${consumiCsvBuilder.headerCsv.getOrElse(List()).mkString(";")}\n" +
      s"${dataMisure.toString.substring(0, 10)};$pod;$pivaId;$pivaUdd;1.0;2.0;3.0;3.0;3.0;3.0;3.0;0.0;0.0;0.0;0.0;0.0;" +
      s"0.0;0.0;0.0;0.0;17.0;0.0;0.0;0.0;21.0;0.0;0.0;0.0;0.0;$ultimaDataAggiornamento"

    Assert.assertEquals(expectedFileName, result.getAs[String](FileSchema.fileName))
    Assert.assertEquals(expectedFileContent, result.getAs[String](FileSchema.fileContent))
    Assert.assertEquals(expectedFileFullName, result.getAs[String](FileSchema.fileFullName))
    Assert.assertEquals(expectedPathRoot, result.getAs[String](FileSchema.filePathRoot))
    Assert.assertEquals(expectedPathSubDirectories, result.getAs[String](FileSchema.filePathSubDirectories))
  }

}
