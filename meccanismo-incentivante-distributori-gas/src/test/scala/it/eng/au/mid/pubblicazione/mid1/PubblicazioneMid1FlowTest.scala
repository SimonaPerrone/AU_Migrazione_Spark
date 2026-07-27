package it.eng.au.mid.pubblicazione.mid1

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.pubblicazione.PubblicazioneMid1Flow
import it.eng.au.mid.model.file.pubblicazione.{FileModel, MidCsvModel, ZipCsvModel, ZipWriterModel}
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, MidAggregatoreInfoModel}
import it.eng.au.mid.schema.file.pubblicazione.{MidCsvSchema, ZipWriterSchema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Date
import java.time.LocalDate

class PubblicazioneMid1FlowTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  def testCalcolaRigheCsv_numeroCsvProdottiMaxRigheCsv(): Unit = {
    val maxRigheCsv = 1
    val mid1DettaglioModel = Seq(
      Mid1DettaglioModel(pdr = "1", contatore = 2, piva_id = "pd1", piva_udd = "pu1", cod_remi = "cr1", gdm = "g4",
        executionid_mid_contatori = 1L, annomese = "202301", executionid = 2L),
      Mid1DettaglioModel(pdr = "2", contatore = 2, piva_id = "pd2", piva_udd = "pu1", cod_remi = "cr1", gdm = "g4",
        executionid_mid_contatori = 1L, annomese = "202301", executionid = 2L)
    ).toDS()

    val result = new PubblicazioneMid1Flow().calcolaRigheCsv(mid1DettaglioModel, maxRigheCsv).cache()

    Assert.assertEquals(2, result.count())
  }

  def testCalcolaRigheCsv_numeroCsvProdottiSplit(): Unit = {
    // verifica che dato un max righe e un CSV potenziale da max righe + 1; allora vengono fatti 2 CSV uno con max righe
    // e l'altro con 1 riga
    val maxRigheCsv = 80000
    val righeInput = maxRigheCsv + 1

    // DS con righeInput righe con lo stesso elemento (tutte le righe devono essere raggruppate nello stesso CSV)
    val mid1DettaglioModel = (1 to righeInput)
      .map(_ => Mid1DettaglioModel(pdr = "1", contatore = 2, piva_id = "pd1", piva_udd = "pu1", cod_remi = "cr1", gdm = "g4",
        executionid_mid_contatori = 1L, annomese = "202301", executionid = 2L)
      ).toDS()

    val result = new PubblicazioneMid1Flow().calcolaRigheCsv(mid1DettaglioModel, maxRigheCsv).cache()
    Assert.assertEquals(maxRigheCsv, result.where(col(MidCsvSchema.progressivo_file) === 1).count())
    Assert.assertEquals(1, result.where(col(MidCsvSchema.progressivo_file) === 2).count())
  }

  def testCalcolaRigheCsv_rigaCalcolata(): Unit = {
    val mid1DettaglioModel = Seq(
      Mid1DettaglioModel(pdr = "1", contatore = 2, piva_id = "pd1", piva_udd = "pu1", cod_remi = "cr1", gdm = "g4",
        executionid_mid_contatori = 1L, annomese = "202301", executionid = 2L, alpha = 2)
    ).toDS()
    val pubblicazioneMid1Flow = new PubblicazioneMid1Flow()

    val expected = Seq("pd1", "pu1", "1", "202301", 2, "cr1", "g4", 2).mkString(pubblicazioneMid1Flow.csvSeparatore)

    val result = pubblicazioneMid1Flow.calcolaRigheCsv(mid1DettaglioModel, 1).head().riga_file

    Assert.assertEquals(expected, result)
  }

  def testDefinisciFileCsv(): Unit = {
    val fileTimestamp = "20231212000000"
    val sessioneForzata = "AGG_S2"

    val midRigheCsv = Seq(
      MidCsvModel(
        piva_distr = "piva_distr",
        piva_udd = "piva_udd",
        riga_file = "abc",
        progressivo_file = 1
      )
    ).toDS()

    val expectedFileModel = FileModel(
      //<PIVA_ID>_<PIVA_UDD>_MID1_<sessione forzata>_<timestamp>_<progressivo>.csv
      fileName = s"piva_distr_piva_udd_MID1_${sessioneForzata}_20231212000000_1.csv",
      content = "PIVA_DISTR;PIVA_UDD;PDR;ANNOMESE;N;COD_REMI;GDM;ALPHA\nabc".getBytes
    )
    val expected = ZipCsvModel(pivaId = "piva_distr", fileModel = expectedFileModel)

    val result = new PubblicazioneMid1Flow().definisciFileCsv(midRigheCsv, fileTimestamp, sessioneForzata).head

    Assert.assertEquals(expected.pivaId, result.pivaId)
    Assert.assertEquals(expected.fileModel.fileName, result.fileModel.fileName)
    Assert.assertEquals(expected.fileModel.content.mkString(""), result.fileModel.content.mkString(""))
  }

  def testDefinisciFileCsv_fileMultipli(): Unit = {
    val fileTimestamp = "20231212000000"
    val sessioneForzata = "AGG_S2"
    val midRigheCsv = Seq(
      MidCsvModel(
        piva_distr = "piva_distr",
        piva_udd = "piva_udd",
        riga_file = "abc",
        progressivo_file = 1
      ),
      MidCsvModel(
        piva_distr = "piva_distr",
        piva_udd = "piva_udd",
        riga_file = "abc",
        progressivo_file = 1
      ),
      MidCsvModel(
        piva_distr = "piva_distr",
        piva_udd = "piva_udd",
        riga_file = "ghi",
        progressivo_file = 2
      )
    ).toDS()

    val result = new PubblicazioneMid1Flow().definisciFileCsv(midRigheCsv, fileTimestamp, sessioneForzata).cache

    Assert.assertEquals(2, result.count())
  }

  def testDefinisciFileCsv_fileCsvCorretto(): Unit = {
    val fileTimestamp = "20231212000000"
    val sessioneForzata = "AGG_S2"
    val midRigheCsv = Seq(
      MidCsvModel(
        piva_distr = "piva_distr",
        piva_udd = "piva_udd",
        riga_file = "abc",
        progressivo_file = 1
      ),
      MidCsvModel(
        piva_distr = "piva_distr",
        piva_udd = "piva_udd",
        riga_file = "def",
        progressivo_file = 1
      )
    ).toDS()

    val expectedFileModel = FileModel(
      fileName = null,
      content = "PIVA_DISTR;PIVA_UDD;PDR;ANNOMESE;N;COD_REMI;GDM;ALPHA\nabc\ndef".getBytes
    )

    val result = new PubblicazioneMid1Flow().definisciFileCsv(midRigheCsv, fileTimestamp, sessioneForzata).cache

    Assert.assertEquals(1, result.count())
    Assert.assertEquals(expectedFileModel.content.mkString, result.head.fileModel.content.mkString)
  }

  def testCalcolaNomeFileZip(): Unit = {
    // verifica correttezza filename
    val percorsoFile = "."
    val dataCalcolo = LocalDate.parse("2024-01-01")
    val fileTimestamp = "20240101000000"
    val annoCalcolo = "2024"
    val meseCalcolo = "01"
    val pivaId = "pivaId"
    val sessioneForzata = "AGG_S2"
    val zipCsv = Seq(
      ZipCsvModel(
        pivaId = pivaId,
        fileModel = null
      )
    ).toDS()
    // percorsoSalvataggio/AGG4_PivaID/ANNO/MESE/PivaID_MID1_SESSIONETRACCITURA_TIMESTAMP_PROGRESSIVO.zip
    val expectedFilePath = s"$percorsoFile/AGG4_$pivaId/$annoCalcolo/$meseCalcolo/${pivaId}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip"
    val result = new PubblicazioneMid1Flow().calcolaNomeFileZip(zipCsv, fileTimestamp, percorsoFile, sessioneForzata, dataCalcolo).head()

    Assert.assertEquals(expectedFilePath, result.zipFileName)
  }

  def testDefinisciFileZip_multiCsv(): Unit = {
    // 3 file csv di cui 2 devono essere salvati all'interno dello stesso zip e uno separato
    val percorsoFile = "."
    val fileTimestamp = "20231231000000"
    val pivaId = "pivaId"
    val anno = "2023"
    val mese = "12"
    val sessioneTracciatura = "SBG"
    val zipFileName = s"$percorsoFile/AGG4_$pivaId/$anno/$mese/${pivaId}_MID1_${sessioneTracciatura}_${fileTimestamp}_1.zip"

    val zipCsv = Seq(
      ZipCsvModel(
        pivaId = pivaId,
        zipFileName = zipFileName,
        fileModel = FileModel(
          fileName = "file1.csv",
          content = "123".getBytes
        )
      ),
      ZipCsvModel(
        pivaId = pivaId,
        zipFileName = zipFileName,
        fileModel = FileModel(
          fileName = "file2.csv",
          content = "345".getBytes
        )
      ),
      // piva differente, atteso in diverso file zip
      ZipCsvModel(
        pivaId = "X",
        zipFileName = "123",
        fileModel = FileModel(
          fileName = "file1.csv",
          content = "123".getBytes
        )
      )
    ).toDS()


    val result = new PubblicazioneMid1Flow().definisciFileZip(zipCsv)

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(2, result.where(col(ZipWriterSchema.fileName) === zipFileName).head().files.length)
    Assert.assertEquals(1, result.where(col(ZipWriterSchema.fileName) =!= zipFileName).head().files.length)
  }

  def testCalcolaAggregatoreInfo(): Unit = {
    val fileZipDs = Seq(ZipWriterModel(pivaId = "1", fileName = "/percorso/filename1.txt", files = null)).toDS()
    val dataCaricamento = Date.valueOf("2023-12-25")
    val executionIdMid1Dettaglio = 0L
    val executionId = 1L

    val expected = MidAggregatoreInfoModel(
      operation_name = CostantiMid.NOME_OPERAZIONE_MID1,
      nome_file = "filename1.txt",
      path = "/percorso/filename1.txt",
      tipo_dest = CostantiMid.DESTINAZIONE_MID1,
      piva_dest = "1",
      piva_id_file = "1",
      piva_udd_file = null,
      data_caricamento = dataCaricamento,
      executionid_mid_dettaglio = executionIdMid1Dettaglio,
      executionid = executionId)

    val result = new PubblicazioneMid1Flow()
      .calcolaAggregatoreInfo(fileZipDs, dataCaricamento, executionIdMid1Dettaglio, executionId)
      .head()
    Assert.assertEquals(expected, result)
  }

}
