package it.eng.au.pubblicazione_cce.pubblicazioni.ammissibilita

import it.eng.au.pubblicazione_cce.EnvironmentSparkTest
import it.eng.au.pubblicazione_cce.common.CostantiCCE._
import it.eng.au.pubblicazione_cce.mock.dao.CceRichiestaPodDaoMock
import it.eng.au.pubblicazione_cce.mock.file.AmmissibilitaPodCsvBuilderMock
import it.eng.au.pubblicazione_cce.mock.flow.PubblicazioneAmmissibilitaPodFlowMock
import it.eng.au.pubblicazione_cce.mock.writer.FileWriterMock
import it.eng.au.pubblicazione_cce.model.cce._
import it.eng.au.pubblicazione_cce.model.file.FileModel
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.SparkSession
import org.junit.Assert

import java.sql.Timestamp
import java.time.LocalDate

class FileAmmissibilitaPodTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  def testFileAmmissibilitaPod(): Unit = {
    //parametri input
    val annoCalcolo = "2024"
    val meseCalcolo = "02"

    val dataRichiesta = Timestamp.valueOf("2024-02-01 00:00:00")
    val dataCalcolo = LocalDate.parse("2024-02-01")
    val anno = "2024"
    val mese = "01"
    val piva = "piva1"
    val fileTimestamp = "20240101000000"
    val outputFilePath = "/"

    val richiestaPodFile = Seq(
      CceRichiestaPodModel(
        n_id_richiesta = "1",
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = dataRichiesta,
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_DISTR,
        t_piva = piva,
        t_codice_pod = "pod1",
        b_ammissibilita = INAMMISSIBILE,
        t_cod_causale = "01",
        t_motivazione = "blabla",
        t_nome_file = "filename.csv",
        t_tipo_amm = RICHIESTA_POD_POD,
        sqoop_date = dataRichiesta,
        partition_request_date = dataRichiesta.toLocalDateTime.toLocalDate.toString
      )).toDS()

    val filePathSubDir = s"$CCE/$PATH_ID/$piva/$PROCESSO_P/$annoCalcolo/$meseCalcolo/"
    val expected = FileModel(
      fileName = s"ReportEsitoPODContatoreConsumi_1_$fileTimestamp.csv",
      filePathRoot = outputFilePath,
      filePathSubDirectories = Some(filePathSubDir),
      fileFullName = s"${filePathSubDir}ReportEsitoPODContatoreConsumi_1_$fileTimestamp.csv",
      fileContent = (
        "Pod;Ammissibilità;Codice_Inammissibilità;Descrizione\n" +
          s"pod1;$INAMMISSIBILE;01;blabla"
        ).getBytes
    )

    val ammissibilitaPodFlowMock = new AmmissibilitaPodCsvBuilderMock(
      fileTimestamp = fileTimestamp, dataCalcolo = dataCalcolo, outputFilePath = outputFilePath
    )

    val flow = new PubblicazioneAmmissibilitaPodFlowMock(
      dataRichieste = dataRichiesta.toLocalDateTime.toLocalDate,
      processo = PROCESSO_P,
      richiestePodDao = new CceRichiestaPodDaoMock(richiestaPodFile),
      outputFileDao = new FileWriterMock(),
      fileTimestamp = fileTimestamp,
      csvBuilder = ammissibilitaPodFlowMock,
      outputFilePath = outputFilePath
    )
    val esito = flow.run().head()

    val result = flow.outputFileDao.asInstanceOf[FileWriterMock].ds.head

    Assert.assertEquals(expected.fileName, result.fileName)
    Assert.assertEquals(expected.fileContent.mkString(""), result.fileContent.mkString(""))

    Assert.assertEquals(s"ReportEsitoPODContatoreConsumi_1_$fileTimestamp.csv", esito.t_file_ammissibilita)
    Assert.assertEquals(outputFilePath + filePathSubDir, esito.t_path)
  }

  def testFileAmmissibilitaFile_complesso(): Unit = {
    //parametri input
    val dataRichiesta = Timestamp.valueOf("2024-02-01 00:00:00")
    val dataCalcolo = LocalDate.parse("2024-02-01")
    val anno = "2024"
    val mese = "01"
    val piva = "piva1"
    val fileTimestamp = "20240101000000"
    val outputFilePath = "/"

    val richiestaPodFile = Seq(
      CceRichiestaPodModel(
        n_id_richiesta = "1",
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = dataRichiesta,
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_DISTR,
        t_piva = piva,
        t_codice_pod = "pod1",
        b_ammissibilita = INAMMISSIBILE,
        t_cod_causale = "01",
        t_motivazione = "blabla",
        t_nome_file = "filename.csv",
        t_tipo_amm = RICHIESTA_POD_POD,
        sqoop_date = dataRichiesta,
        partition_request_date = dataRichiesta.toLocalDateTime.toLocalDate.toString
      ),
      CceRichiestaPodModel(
        n_id_richiesta = "2",
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = dataRichiesta,
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_DISTR,
        t_piva = piva,
        t_codice_pod = "pod2",
        b_ammissibilita = AMMISSIBILE,
        t_cod_causale = null,
        t_motivazione = null,
        t_nome_file = "filenameok.csv",
        t_tipo_amm = RICHIESTA_POD_POD,
        sqoop_date = dataRichiesta,
        partition_request_date = dataRichiesta.toLocalDateTime.toLocalDate.toString
      )
    ).toDS()

    val ammissibilitaPodFlowMock = new AmmissibilitaPodCsvBuilderMock(
      fileTimestamp = fileTimestamp, dataCalcolo = dataCalcolo, outputFilePath = outputFilePath
    )

    val flow = new PubblicazioneAmmissibilitaPodFlowMock(
      dataRichieste = dataRichiesta.toLocalDateTime.toLocalDate,
      processo = PROCESSO_P,
      richiestePodDao = new CceRichiestaPodDaoMock(richiestaPodFile),
      outputFileDao = new FileWriterMock(),
      fileTimestamp = fileTimestamp,
      csvBuilder = ammissibilitaPodFlowMock,
      outputFilePath = outputFilePath
    )
    flow.run()

    val result = flow.outputFileDao.asInstanceOf[FileWriterMock].ds

    Assert.assertEquals(2, result.count())
  }

  def testFileAmmissibilitaFile_richiestaPodMultipli(): Unit = {
    //parametri input
    val dataRichiesta = Timestamp.valueOf("2024-02-01 00:00:00")
    val dataCalcolo = LocalDate.parse("2024-02-01")
    val anno = "2024"
    val mese = "01"
    val piva = "piva1"
    val fileTimestamp = "20240101000000"
    val outputFilePath = "/"

    val richiestaPodFile = Seq(
      CceRichiestaPodModel(
        n_id_richiesta = "1",
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = dataRichiesta,
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_DISTR,
        t_piva = piva,
        t_codice_pod = "pod1",
        b_ammissibilita = INAMMISSIBILE,
        t_cod_causale = "01",
        t_motivazione = "blabla",
        t_nome_file = "filename.csv",
        t_tipo_amm = RICHIESTA_POD_POD,
        sqoop_date = dataRichiesta,
        partition_request_date = dataRichiesta.toLocalDateTime.toLocalDate.toString
      ),
      CceRichiestaPodModel(
        n_id_richiesta = "1",
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = dataRichiesta,
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_DISTR,
        t_piva = piva,
        t_codice_pod = "pod2",
        b_ammissibilita = AMMISSIBILE,
        t_cod_causale = null,
        t_motivazione = null,
        t_nome_file = "filenameok.csv",
        t_tipo_amm = RICHIESTA_POD_POD,
        sqoop_date = dataRichiesta,
        partition_request_date = dataRichiesta.toLocalDateTime.toLocalDate.toString
      )
    ).toDS()

    val ammissibilitaPodFlowMock = new AmmissibilitaPodCsvBuilderMock(
      fileTimestamp = fileTimestamp, dataCalcolo = dataCalcolo, outputFilePath = outputFilePath
    )

    val flow = new PubblicazioneAmmissibilitaPodFlowMock(
      dataRichieste = dataRichiesta.toLocalDateTime.toLocalDate,
      processo = PROCESSO_P,
      richiestePodDao = new CceRichiestaPodDaoMock(richiestaPodFile),
      outputFileDao = new FileWriterMock(),
      fileTimestamp = fileTimestamp,
      csvBuilder = ammissibilitaPodFlowMock,
      outputFilePath = outputFilePath
    )
    flow.run()

    val result = flow.outputFileDao.asInstanceOf[FileWriterMock].ds

    Assert.assertEquals(1, result.count())
  }

}
