package it.eng.au.pubblicazione_cce.pubblicazioni

import it.eng.au.pubblicazione_cce.EnvironmentSparkTest
import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.common.CostantiCCE._
import it.eng.au.pubblicazione_cce.mock.dao._
import it.eng.au.pubblicazione_cce.mock.flow.PubblicazioneFlowMock
import it.eng.au.pubblicazione_cce.model.cce._
import it.eng.au.pubblicazione_cce.schema.cce.CceEsitoSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp
import java.time.LocalDate


// Test pubblicazione completo. Builder di file sono mockati ma possibile produrre file anche su unit test
// vedi test sotto cartella "writer" che scrivono file su file system locale
class PubblicazioneTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  def testCalcoloP(): Unit = {
    //parametri input
    val dataRichiesta = LocalDate.parse("2024-01-01")
    val dataMisure = Timestamp.valueOf("2024-01-31 00:00:00")
    val dataCalcolo = LocalDate.parse("2024-02-02")
    val annoCalcolo = "2024"
    val meseCalcolo = "02"
    val anno = "2024"
    val mese = "01"
    val annomese = anno + mese
    val piva = "piva1"
    val pod = "pod1"
    val pivaId = "pivaID1"
    val pivaUdd = "pivaUDD1"
    val tariffa = "TD"
    val tipoPod = "G"
    val timestampProcesso = Timestamp.valueOf("2024-02-02 10:10:10")
    val codiceTerna = "terna"
    val ultimaDataAggiornamento = dataRichiesta.toString
    val executionid = "execid"
    val executionidMisure = "execid_misure"
    val fileTimestamp = "20240102000000"
    val outputFilePath = "/"
    val nomeFile = "file.xml"

    //input
    val richiestaPodDs = Seq(
      CceRichiestaPodModel(
        n_id_richiesta = "1",
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_SII,
        t_piva = piva,
        t_codice_pod = pod,
        b_ammissibilita = AMMISSIBILE,
        t_cod_causale = null,
        t_motivazione = null,
        t_nome_file = null,
        t_tipo_amm = RICHIESTA_POD_POD,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      ),
      // richiesta FIlE inammissibile
      CceRichiestaPodModel(
        n_id_richiesta = "3",
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_SII,
        t_piva = piva,
        t_codice_pod = null,
        b_ammissibilita = INAMMISSIBILE,
        t_cod_causale = null,
        t_motivazione = null,
        t_nome_file = null,
        t_tipo_amm = RICHIESTA_POD_FILE,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      )
    ).toDS()

    val richiestaFiltro = Seq(
      CceRichiestaFiltroModel(
        n_id_richiesta = "2",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_SII,
        t_piva = piva,
        t_tensione = TENSIONE_MEDIA,
        t_zona = ZONA_NORD,
        t_tipo_pod = tipoPod,
        t_piva_udd = null,
        t_piva_id = null,
        t_codice_terna = codiceTerna,
        t_tariffa = tariffa,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      ),
      // richiesta filtro no consumi trovati
      CceRichiestaFiltroModel(
        n_id_richiesta = "4",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_UDD,
        t_piva = piva,
        t_tensione = TENSIONE_MEDIA,
        t_zona = ZONA_SUD,
        t_tipo_pod = tipoPod,
        t_piva_udd = null,
        t_piva_id = null,
        t_codice_terna = codiceTerna,
        t_tariffa = tariffa,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      )
    ).toDS()

    val anagraficaDs = Seq(
      CceCalcoloAnagraficaModel(
        t_codice_pod = pod,
        n_id_pod = 123,
        t_area_rif = ZONA_NORD,
        n_tensione = "123",
        t_tensione = TENSIONE_MEDIA,
        t_tipo_pod = tipoPod,
        t_piva_id = pivaId,
        t_piva_udd = pivaUdd,
        d_inizio_udd = "2017-02-02 00:00:00.0",
        d_fine_udd = "3000-12-31",
        t_codice_terna = codiceTerna,
        t_tariffa_distr = "TD"
      )).toDS()

    val trattamento = Seq(
      CceCalcoloTrattamentoModel(
        t_codice_pod = pod,
        n_id_pod = null,
        t_anno_mese = annomese,
        d_data_elaborazione = dataRichiesta.toString,
        is_t_trattamento = "Y"
      )).toDS()

    val calcTrack = Seq(
      CceCalcTrackModel(
        t_tipo_calc = PROCESSO_P,
        t_mode_calc = null,
        t_anno_calc = anno,
        t_mese_calc = mese,
        d_data_calc = dataRichiesta.toString,
        t_esito = ESITO_OK,
        executionid = executionidMisure
      )).toDS()

    val calcoloMisureDs = Seq(
      CceCalcoloMisureModel(
      pod = pod,
      data_misura = dataMisure.toString.substring(0, 10),
      giorno = "31",
      h01 = 1.0, h02 = 2.0, h03 = 3.0, h04 = 3.0, h05 = 3.0, h06 = 3.0, h07 = 3.0, h17 = 17.0, h21 = 21.0,
      data_calcolo = dataRichiesta.toString,
      nome_file = nomeFile,
      anno = anno,
      mese = mese,
      executionid = executionidMisure
    ),
      CceCalcoloMisureModel(
      pod = "altroPod",
      data_misura = dataMisure.toString.substring(0, 10),
      giorno = "30",
      h01 = 1.0, h02 = 2.0, h03 = 3.0, h04 = 3.0, h05 = 3.0, h06 = 3.0, h07 = 3.0, h17 = 17.0, h21 = 21.0,
      data_calcolo = dataRichiesta.toString,
      nome_file = nomeFile,
      anno = anno,
      mese = mese,
      executionid = executionidMisure
    )).toDS()


    val esitoDao = new CceEsitoDaoMock

    val flow = new PubblicazioneFlowMock(
      dataRichieste = dataRichiesta,
      processo = CostantiCCE.PROCESSO_P,
      richiestaFiltroDao = new CceRichiestaFiltroDaoMock(richiestaFiltro),
      dataCalcolo = dataCalcolo,
      processTimestamp = timestampProcesso,
      esitoDao = esitoDao,
      executionId = executionid,
      richiestaPodDao = new CceRichiestaPodDaoMock(richiestaPodDs),
      anagraficaPodDao = new CceCalcoloAnagraficaDaoMock(anagraficaDs),
      misureDao = new CceCalcoloDaoMock(calcoloMisureDs),
      trattamentoDao = new CceCalcoloTrattamentoDaoMock(trattamento),
      trackDao = new CceCalcTrackDaoMock(calcTrack),
      outputFilePath = outputFilePath,
      fileTimestamp = fileTimestamp,
      maxLineCsv = 10
    )
    flow.run()

    val ds = flow.esitoDao.asInstanceOf[CceEsitoDaoMock].ds

    val expectedEsitoPod = CceEsitoModel(
      n_id_richiesta = "1",
      t_path = s"/$CCE/$PATH_ID/$piva/$PROCESSO_P/$annoCalcolo/$meseCalcolo/",
      t_file_esito = s"${piva}_${CCE}_${PROCESSO_P}_$anno${mese}_${fileTimestamp}_1_1.zip",
      t_file_ammissibilita = s"ReportEsitoPODContatoreConsumi_1_$fileTimestamp.csv",
      t_stato = STATO_ELABORATO,
      t_operation_name = PROCESSO_P,
      t_number_file_zip = 1,
      execution_id_input_read = executionidMisure,
      d_data_esito = timestampProcesso,
      tipo_richiesta = RICHIESTA_POD,
      n_executionid = executionid,
      d_data_richiesta = dataRichiesta.toString
    )

    val expectedEsitoFiltro = CceEsitoModel(
      n_id_richiesta = "2",
      t_path = s"/$CCE/$PATH_ID/$piva/$PROCESSO_P/$annoCalcolo/$meseCalcolo/",
      t_file_esito = s"${piva}_${CCE}_${PROCESSO_P}_$anno${mese}_${fileTimestamp}_2_1.zip",
      t_file_ammissibilita = null,
      t_stato = STATO_ELABORATO,
      t_operation_name = PROCESSO_P,
      t_number_file_zip = 1,
      execution_id_input_read = executionidMisure,
      d_data_esito = timestampProcesso,
      tipo_richiesta = RICHIESTA_FILTRO,
      n_executionid = executionid,
      d_data_richiesta = dataRichiesta.toString
    )

    val expectedEsitoPodInammissibile = CceEsitoModel(
      n_id_richiesta = "3",
      t_path = s"/$CCE/$PATH_ID/$piva/$PROCESSO_P/$annoCalcolo/$meseCalcolo/",
      t_file_esito = null,
      t_file_ammissibilita = s"ReportEsitoFileContatoreConsumi_3_$fileTimestamp.csv",
      t_stato = STATO_NON_AMMISSIBILE,
      t_operation_name = PROCESSO_P,
      t_number_file_zip = 0,
      execution_id_input_read = null,
      d_data_esito = timestampProcesso,
      tipo_richiesta = RICHIESTA_POD,
      n_executionid = executionid,
      d_data_richiesta = dataRichiesta.toString
    )

    val expectedEsitoFiltroNoConsumi = CceEsitoModel(
      n_id_richiesta = "4",
      t_path = s"/$CCE/$PATH_UDD/$piva/$PROCESSO_P/$annoCalcolo/$meseCalcolo/",
      t_file_esito = null,
      t_file_ammissibilita = null,
      t_stato = STATO_NO_CONSUMI,
      t_operation_name = PROCESSO_P,
      t_number_file_zip = 0,
      execution_id_input_read = null,
      d_data_esito = timestampProcesso,
      tipo_richiesta = RICHIESTA_FILTRO,
      n_executionid = executionid,
      d_data_richiesta = dataRichiesta.toString
    )

    //output
    Assert.assertEquals(expectedEsitoPod, ds.where(col(CceEsitoSchema.n_id_richiesta) === "1").head())
    Assert.assertEquals(expectedEsitoFiltro, ds.where(col(CceEsitoSchema.n_id_richiesta) === "2").head())
    Assert.assertEquals(expectedEsitoPodInammissibile, ds.where(col(CceEsitoSchema.n_id_richiesta) === "3").head())
    Assert.assertEquals(expectedEsitoFiltroNoConsumi, ds.where(col(CceEsitoSchema.n_id_richiesta) === "4").head())
  }

}
