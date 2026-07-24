package it.eng.au.pubblicazione_cce.pubblicazioni.P

import it.eng.au.pubblicazione_cce.EnvironmentSparkTest
import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.common.CostantiCCE._
import it.eng.au.pubblicazione_cce.mock.flow.PubblicazioneConsumiFlowMock
import it.eng.au.pubblicazione_cce.model.cce._
import it.eng.au.pubblicazione_cce.model.file.FileConsumiModel
import it.eng.au.pubblicazione_cce.model.flow.{CalcTrackAggModel, PodPubblicazioneModel}
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_utc_timestamp}
import org.apache.spark.sql.types.TimestampType
import org.junit.Assert

import java.sql.Timestamp
import java.time.LocalDate

// Testa la funzione calcolaPodRichiesteFiltro e calcolaMisurePod per estrarre i consumi da richieste filtro applicando le logiche richieste
class PubblicazioneCalcoloTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /// DATI VALORIZZAZIONE TESTS ///

  val dataRichiesta = LocalDate.parse("2024-01-01")
  val dataMisure = Timestamp.valueOf("2024-01-31 00:00:00")
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
  val ultimaDataAggiornamento = "2023-12-31T09:37:34.602"
  val ultimaDataAggiornamentoString = "20231231093734"
  val executionid = "execid"

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
      d_inizio_udd = "2023-01-01 00:00:00.0",
      d_fine_udd = "3000-12-31",
      t_codice_terna = codiceTerna,
      t_tariffa_distr = "TD"
    )
  ).toDS()

  val trattamento = Seq(
    CceCalcoloTrattamentoModel(
      t_codice_pod = pod,
      n_id_pod = null,
      t_anno_mese = annomese,
      d_data_elaborazione = dataRichiesta.toString,
      is_t_trattamento = "Y"
    )).toDS()

  //calcoloMisure()
  val calcoloMisureDs = Seq(
    CceCalcoloMisureModel(
      pod = pod,
      data_misura = dataMisure.toString.substring(0, 10),
      giorno = dataMisure.toString.substring(9, 10),
      h01 = 1.0, h02 = 2.0, h03 = 3.0, h04 = 3.0, h05 = 3.0, h06 = 3.0, h07 = 3.0, h17 = 17.0, h21 = 21.0,
      data_calcolo = ultimaDataAggiornamento,
      nome_file = "file1.xml",
      anno = anno,
      mese = mese,
      executionid = executionid
    ),
    CceCalcoloMisureModel(
      pod = "altroPod",
      data_misura = dataMisure.toString.substring(0, 10),
      giorno = dataMisure.toString.substring(0, 10),
      h01 = 1.0, h02 = 2.0, h03 = 3.0, h04 = 3.0, h05 = 3.0, h06 = 3.0, h07 = 3.0, h17 = 17.0, h21 = 21.0,
      data_calcolo = ultimaDataAggiornamento,
      nome_file = "file1.xml",
      anno = anno,
      mese = mese,
      executionid = executionid
    )).toDS()

  val trackDs = Seq(CalcTrackAggModel(
    t_anno_calc = anno,
    t_mese_calc = mese,
    d_data_calc = "",
    executionid = executionid
  )).toDS()

  val pubblicazioneConsumiFlow = new PubblicazioneConsumiFlowMock(
    dataRichieste = dataRichiesta,
    processo = CostantiCCE.PROCESSO_P,
    richiestePodDao = null,
    richiesteFiltroDao = null,
    anagraficaPodDao = null,
    misureDao = null,
    trattamentoDao = null,
    trackDao = null,
    outputFileCsvWriter = null,
    consumiCsvBuilder = null,
    elencoFileCsvBuilder = null,
    processTimestamp = timestampProcesso,
    fileTimestamp = ""
  )


  /// INIZIO TESTS ///

  // test valorizzazione misure
  def testRichiesteFiltroMisure(): Unit = {
    //parametri input
    val richiestaFiltro = Seq(
      CceRichiestaFiltroModel(
        n_id_richiesta = "1",
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
        t_piva_udd = pivaUdd,
        t_piva_id = pivaId,
        t_codice_terna = codiceTerna,
        t_tariffa = tariffa,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      )).toDS()

    val expected = PodPubblicazioneModel(
      piva = piva,
      ruolo = RUOLO_SII,
      sessione = CCE1,
      processo = PROCESSO_P,
      anno = anno,
      mese = mese,
      id_richiesta = "1",
      cod_pod = pod,
      piva_distr = pivaId,
      piva_udd = pivaUdd,
      d_inizio_udd = "2023-01-01 00:00:00.0",
      d_fine_udd = "3000-12-31",
      d_data_calc = "",
      executionid = executionid
    )

    val result = pubblicazioneConsumiFlow.calcolaPodRichiesteFiltro(
      filtro = richiestaFiltro,
      anagrafica = anagraficaDs,
      track = trackDs
    )

    //output
    Assert.assertEquals(expected, result.head)
  }

  def testCalcolaMisurePod(): Unit = {
    val podPubblicazione = Seq(
      PodPubblicazioneModel(
        piva = piva,
        ruolo = RUOLO_SII,
        sessione = CCE1,
        processo = PROCESSO_P,
        anno = anno,
        mese = mese,
        id_richiesta = "1",
        cod_pod = pod,
        piva_distr = pivaId,
        piva_udd = pivaUdd,
        d_inizio_udd = "2023-01-01 00:00:00.0",
        d_fine_udd = "3000-12-31",
        d_data_calc = "",
        executionid = executionid
      )).toDS

    val expected = FileConsumiModel(
      piva = piva,
      ruolo = RUOLO_SII,
      sessione = CCE1,
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
      data_aggiornamento = ultimaDataAggiornamentoString,
      nome_file = "file1.xml",
      executionid = executionid
    )

    val result = pubblicazioneConsumiFlow.calcolaMisurePod(podPubblicazione, misure = calcoloMisureDs, trattamento= trattamento)

    Assert.assertEquals(expected, result.head())

  }

  // test filtro per ruolo distr
  def testRichiesteFiltroMisure_ruoloDistr(): Unit = {
    //parametri input
    val richiestaFiltro = Seq(
      // t_piva uguale da pivaId anagrafica -> mantenere
      CceRichiestaFiltroModel(
        n_id_richiesta = "1",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_DISTR,
        t_piva = pivaId,
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
      // t_piva diverso da pivaId anagrafica -> scartare
      CceRichiestaFiltroModel(
        n_id_richiesta = "2",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_DISTR,
        t_piva = "PIVA NON COERENTE",
        t_tensione = TENSIONE_MEDIA,
        t_zona = ZONA_NORD,
        t_tipo_pod = tipoPod,
        t_piva_udd = null,
        t_piva_id = null,
        t_codice_terna = codiceTerna,
        t_tariffa = tariffa,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      )
    ).toDS()

    val result = pubblicazioneConsumiFlow.calcolaPodRichiesteFiltro(
      filtro = richiestaFiltro,
      anagrafica = anagraficaDs,
      track = trackDs
    )

    //output
    Assert.assertEquals(1, result.count())
    Assert.assertEquals("1", result.head.id_richiesta)
  }

  // test filtro per ruolo udd
  def testRichiesteFiltroMisure_ruoloUdd(): Unit = {
    //parametri input
    val richiestaFiltro = Seq(
      // t_piva uguale da pivaUdd anagrafica -> mantenere
      CceRichiestaFiltroModel(
        n_id_richiesta = "1",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_UDD,
        t_piva = pivaUdd,
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
      // t_piva diverso da pivaUdd anagrafica -> scartare
      CceRichiestaFiltroModel(
        n_id_richiesta = "2",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_UDD,
        t_piva = "PIVA NON COERENTE",
        t_tensione = TENSIONE_MEDIA,
        t_zona = ZONA_NORD,
        t_tipo_pod = tipoPod,
        t_piva_udd = null,
        t_piva_id = null,
        t_codice_terna = codiceTerna,
        t_tariffa = tariffa,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      )
    ).toDS()

    val result = pubblicazioneConsumiFlow.calcolaPodRichiesteFiltro(
      filtro = richiestaFiltro,
      anagrafica = anagraficaDs,
      track = trackDs
    )

    //output
    Assert.assertEquals(1, result.count())
    Assert.assertEquals("1", result.head.id_richiesta)
  }

  // test filtro per t piva id
  def testRichiesteFiltroMisure_filtroTPivaId(): Unit = {
    //parametri input
    val richiestaFiltro = Seq(
      // t_piva_id valida per filtro -> mantenere
      CceRichiestaFiltroModel(
        n_id_richiesta = "1",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_UDD,
        t_piva = pivaUdd,
        t_tensione = TENSIONE_MEDIA,
        t_zona = ZONA_NORD,
        t_tipo_pod = tipoPod,
        t_piva_udd = null,
        t_piva_id = pivaId, /// QUESTO FILTRO!
        t_tariffa = tariffa,
        t_codice_terna = codiceTerna,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      ),
      // t_piva_id non valida per filtro -> scartare
      CceRichiestaFiltroModel(
        n_id_richiesta = "2",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_UDD,
        t_piva = pivaUdd,
        t_tensione = TENSIONE_MEDIA,
        t_zona = ZONA_NORD,
        t_tipo_pod = tipoPod,
        t_piva_udd = null,
        t_piva_id = "altra piva non valida",
        t_tariffa = tariffa,
        t_codice_terna = codiceTerna,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      )
    ).toDS()

    val result = pubblicazioneConsumiFlow.calcolaPodRichiesteFiltro(
      filtro = richiestaFiltro,
      anagrafica = anagraficaDs,
      track = trackDs
    )

    //output
    Assert.assertEquals(1, result.count())
    Assert.assertEquals("1", result.head.id_richiesta)
  }

  // test filtro per t piva udd (equivalente a test precedente solo per piva UDD)
  def testRichiesteFiltroMisure_filtroTPivaUdd(): Unit = {
    //parametri input
    val richiestaFiltro = Seq(
      // t_piva_id valida per filtro -> mantenere
      CceRichiestaFiltroModel(
        n_id_richiesta = "1",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_UDD,
        t_piva = pivaUdd,
        t_tensione = TENSIONE_MEDIA,
        t_zona = ZONA_NORD,
        t_tipo_pod = tipoPod,
        t_piva_udd = pivaUdd, /// QUESTO FILTRO!
        t_piva_id = null,
        t_codice_terna = codiceTerna,
        t_tariffa = tariffa,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      ),
      // t_piva_id non valida per filtro -> scartare
      CceRichiestaFiltroModel(
        n_id_richiesta = "2",
        t_tipo = RICHIESTA_FILTRO,
        t_servizio = CCE1,
        t_processo = PROCESSO_P,
        d_data_richiesta = Timestamp.valueOf(dataRichiesta.atStartOfDay()),
        t_anno = anno,
        t_mese = mese,
        t_ruolo = RUOLO_UDD,
        t_piva = pivaUdd,
        t_tensione = TENSIONE_MEDIA,
        t_zona = ZONA_NORD,
        t_tipo_pod = tipoPod,
        t_piva_udd = "altra piva non valida",
        t_piva_id = null,
        t_codice_terna = codiceTerna,
        t_tariffa = tariffa,
        sqoop_date = null,
        partition_request_date = dataRichiesta.toString
      )
    ).toDS()

    val result = pubblicazioneConsumiFlow.calcolaPodRichiesteFiltro(
      filtro = richiestaFiltro,
      anagrafica = anagraficaDs,
      track = trackDs
    )

    //output
    Assert.assertEquals(1, result.count())
    Assert.assertEquals("1", result.head.id_richiesta)
  }

}
