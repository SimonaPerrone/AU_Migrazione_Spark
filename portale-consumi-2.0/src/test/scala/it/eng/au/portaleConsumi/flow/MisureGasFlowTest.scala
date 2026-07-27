package it.eng.au.portaleConsumi.flow

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.flow.misure.{FornitureMisureGasArricchiteModel, FornitureMisureGasDeltaModel, FornitureMisureGasGruppoModel, MisureGasModel}
import it.eng.au.portaleConsumi.model.hive.misuregas._
import it.eng.au.portaleConsumi.model.mongodb.forniture.{AutoletturaDettaglio, MensileDettaglio, MisuraDettaglio, VolturaDettaglio}
import it.eng.au.portaleConsumi.schema.flow.misure.{FornitureMisureGasDeltaSchema, FornitureMisureGasGruppoSchema}
import it.eng.au.portaleConsumi.schema.misuregas.{FornitureMisureGasSchema, FornitureProcessiGasSchema}
import it.eng.au.portaleConsumi.utility.common.Costanti._
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class MisureGasFlowTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark

  import spark.implicits._

  def testLeggiForniture(): Unit = {
    val ds = Seq(
      FornitureProcessiGasModel(codice_fornitura = "1", data_inizio_fornitura = Timestamp.valueOf("2020-01-01 00:00:00")),
      FornitureProcessiGasModel(codice_fornitura = "2", data_inizio_fornitura = Timestamp.valueOf("2023-01-01 00:00:00")),
      FornitureProcessiGasModel(codice_fornitura = "3", data_inizio_fornitura = Timestamp.valueOf("2020-01-01 00:00:00"),
        data_fine_fornitura = Timestamp.valueOf("2022-01-01 00:00:00")),
      //NO
      FornitureProcessiGasModel(codice_fornitura = "x1", data_inizio_fornitura = Timestamp.valueOf("2023-02-01 00:00:00")),
      FornitureProcessiGasModel(codice_fornitura = "x2", data_inizio_fornitura = Timestamp.valueOf("2019-02-01 00:00:00"),
        data_fine_fornitura = Timestamp.valueOf("2019-12-02 00:00:00"))
    ).toDS()

    val result = new MisureGasFlow().filtraFornitureNelPeriodo(ds, "202001", "202301").cache()

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(0, result.where(col(FornitureProcessiGasSchema.codice_fornitura).isin("x1", "x2")).count())
  }

  def testAssociaMisureConForniture(): Unit = {
    val fornitureDs = Seq(
      FornitureProcessiGasModel(
        codice_fiscale = "cf1",
        codice_pdr = "pdr1",
        p_iva = "piva1",
        codice_fornitura = "forn1",
        data_inizio_fornitura = Timestamp.valueOf("2020-01-01 00:00:00"),
        data_fine_fornitura = Timestamp.valueOf("2020-12-31 23:59:59")
      ),
      FornitureProcessiGasModel(
        codice_fiscale = "cf1",
        codice_pdr = "pdr1",
        p_iva = "piva1",
        codice_fornitura = "forn2",
        data_inizio_fornitura = Timestamp.valueOf("2021-01-01 00:00:00"),
        data_fine_fornitura = null
      )
    ).toDS()
    val misureGasDs = Seq(
      MisureGasModel(codice_pdr = "pdr1", lettura = 1, data_lettura = Timestamp.valueOf("2020-06-01 00:00:00"),
        data_caricamento = Timestamp.valueOf("2020-06-01 00:00:00"), annomese = "202006", flusso = TML),
      MisureGasModel(codice_pdr = "pdr1", lettura = 5, data_lettura = Timestamp.valueOf("2021-06-01 00:00:00"),
        data_caricamento = Timestamp.valueOf("2021-06-01 00:00:00"), annomese = "202106", flusso = TML),
      // misura di una fornitura non esistente in fornitureDs ma con PDR esistente
      MisureGasModel(codice_pdr = "pdr1", lettura = 3, data_lettura = Timestamp.valueOf("2018-06-01 00:00:00"),
        data_caricamento = Timestamp.valueOf("2019-06-01 00:00:00"), annomese = "201806", flusso = TML),
      // misura senza PDR, da escludere
      MisureGasModel(codice_pdr = "X", lettura = 0, data_lettura = Timestamp.valueOf("2019-06-01 00:00:00"),
        data_caricamento = Timestamp.valueOf("2019-06-01 00:00:00"), annomese = "201906", flusso = TML)
    ).toDS()

    val expected1 = FornitureMisureGasModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 1, data_lettura = Timestamp.valueOf("2020-06-01 00:00:00"),
      motivazione = null, data_caricamento = Timestamp.valueOf("2020-06-01 00:00:00"), annomese = "202006", flusso = TML)
    val expected2 = FornitureMisureGasModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn2", lettura = 5,
      data_lettura = Timestamp.valueOf("2021-06-01 00:00:00"), motivazione = null,
      data_caricamento = Timestamp.valueOf("2021-06-01 00:00:00"), annomese = "202106", flusso = TML)
    val expected3 = FornitureMisureGasModel(codice_pdr = "pdr1", lettura = 3,
      data_lettura = Timestamp.valueOf("2018-06-01 00:00:00"),
      data_caricamento = Timestamp.valueOf("2019-06-01 00:00:00"), annomese = "201806", flusso = TML)

    val result = new MisureGasFlow().associaMisureConForniture(fornitureDs, misureGasDs).cache()

    Assert.assertEquals(expected1, result.where(col(FornitureMisureGasSchema.codice_fornitura) === "forn1").head())
    Assert.assertEquals(expected2, result.where(col(FornitureMisureGasSchema.codice_fornitura) === "forn2").head())
    Assert.assertEquals(expected3, result.where(col(FornitureMisureGasSchema.codice_fornitura).isNull).head())
    Assert.assertEquals(3, result.count())
  }

  def testCalcolaMisureRiempimento(): Unit = {
    val dataCalcolo = Timestamp.valueOf("2023-01-01 00:00:00")

    val fornitureDs = Seq(
      FornitureProcessiGasModel(
        codice_fiscale = "cf1",
        codice_pdr = "pdr1",
        codice_fornitura = "forn1",
        data_inizio_fornitura = Timestamp.valueOf("2020-01-01 00:00:00"),
        data_fine_fornitura = Timestamp.valueOf("2020-12-31 00:00:00"),
        data_aggiornamento = Timestamp.valueOf("2021-04-02 12:17:13.0")
      )
    ).toDS()

    val misureDs = Seq(
      FornitureMisureGasGruppoModel(codice_fiscale = "cf1", codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 1)
    ).toDS()

    val expected1 = FornitureMisureGasGruppoModel(
      codice_fiscale = "cf1", codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = null, annomese = "202001",
      flusso = _MAN, gruppo_flusso = 1, gruppo_periodo_competenza = "202001", gruppo_priorita = 9, riempimento = 1,
      data_caricamento = Timestamp.valueOf("2021-04-02 12:17:13.0")
    )
    val expected2 = FornitureMisureGasGruppoModel(
      codice_fiscale = "cf1", codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = null,
      data_lettura = Timestamp.valueOf("2020-12-31 00:00:00"), annomese = "202012", flusso = _MAN, gruppo_flusso = 1,
      gruppo_periodo_competenza = "202012", gruppo_priorita = 9, riempimento = 1, data_caricamento = Timestamp.valueOf("2021-04-02 12:17:13.0")
    )

    val result = new MisureGasFlow()
      .calcolaMisureRiempimentoInizioFineFornitura(misureDs, fornitureDs, "199001", "299901", dataCalcolo).cache()

    Assert.assertEquals(expected1, result.where(col(FornitureMisureGasGruppoSchema.annomese) === "202001").head())
    Assert.assertEquals(expected2, result.where(col(FornitureMisureGasGruppoSchema.annomese) === "202012").head())
  }

  def testCalcolaPrioritaMisure(): Unit = {
    val rml = FornitureMisureGasModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 1, data_lettura = Timestamp.valueOf("2020-06-01 00:00:00"), motivazione = "1",
      data_caricamento = Timestamp.valueOf("2020-06-01 00:00:00"), annomese = "202006", flusso = RML)
    val tml = FornitureMisureGasModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 1, data_lettura = Timestamp.valueOf("2020-06-01 00:00:00"), motivazione = null,
      data_caricamento = Timestamp.valueOf("2020-06-01 00:00:00"), annomese = "202006", flusso = TML)
    // giornaliere: tgl rgl: su codice fornitura e data_lettura, rgl su tgl
    val tgl = FornitureMisureGasModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 1, data_lettura = Timestamp.valueOf("2021-01-01 00:00:00"), motivazione = null,
      data_caricamento = Timestamp.valueOf("2021-01-01 00:00:00"), annomese = "202101", flusso = TGL)
    val rgl = FornitureMisureGasModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 2, data_lettura = Timestamp.valueOf("2021-01-01 00:00:00"), motivazione = "2",
      data_caricamento = Timestamp.valueOf("2021-01-01 00:00:00"), annomese = "202101", flusso = RGL)
    // volture: vtg, rmv su codice fornitura e annomese, rmv su vtg
    val vtg = FornitureMisureGasModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 0, data_lettura = Timestamp.valueOf("2020-11-01 00:00:00"), motivazione = null,
      data_caricamento = Timestamp.valueOf("2020-11-01 00:00:00"), annomese = "202011", flusso = VTG)
    val rmv = FornitureMisureGasModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 1, data_lettura = Timestamp.valueOf("2020-11-02 00:00:00"), motivazione = "3",
      data_caricamento = Timestamp.valueOf("2020-11-02 00:00:00"), annomese = "202011", flusso = RMV)

    val fornitureMisureGasDs = Seq(rml, tml, tgl, rgl, vtg, rmv).toDS()

    val misureGasFlow = new MisureGasFlow()

    val expectedRml = FornitureMisureGasGruppoModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 1, data_lettura = Timestamp.valueOf("2020-06-01 00:00:00"), motivazione = "1",
      data_caricamento = Timestamp.valueOf("2020-06-01 00:00:00"), annomese = "202006", flusso = RML, data_calcolo = "2020-01-01",
      riempimento = 0, gruppo_flusso = GRUPPO_MISURE_MENSILI, gruppo_periodo_competenza = "202006", gruppo_priorita = 0)

    val expectedTml = FornitureMisureGasGruppoModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 1, data_lettura = Timestamp.valueOf("2020-06-01 00:00:00"), motivazione = null,
      data_caricamento = Timestamp.valueOf("2020-06-01 00:00:00"), annomese = "202006", flusso = TML, data_calcolo = "2020-01-01",
      riempimento = 0, gruppo_flusso = GRUPPO_MISURE_MENSILI, gruppo_periodo_competenza = "202006", gruppo_priorita = 1)

    val expectedTgl = FornitureMisureGasGruppoModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 1, data_lettura = Timestamp.valueOf("2021-01-01 00:00:00"), motivazione = null,
      data_caricamento = Timestamp.valueOf("2021-01-01 00:00:00"), annomese = "202101", flusso = TGL, data_calcolo = "2020-01-01",
      riempimento = 0, gruppo_flusso = GRUPPO_MISURE_GIORNALIERE, gruppo_periodo_competenza = "2021-01-01 00:00:00",
      gruppo_priorita = 1)

    val expectedRgl = FornitureMisureGasGruppoModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 2, data_lettura = Timestamp.valueOf("2021-01-01 00:00:00"), motivazione = "2",
      data_caricamento = Timestamp.valueOf("2021-01-01 00:00:00"), annomese = "202101", flusso = RGL, data_calcolo = "2020-01-01",
      riempimento = 0, gruppo_flusso = GRUPPO_MISURE_GIORNALIERE, gruppo_periodo_competenza = "2021-01-01 00:00:00",
      gruppo_priorita = 0)

    val expectedVtg = FornitureMisureGasGruppoModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 0, data_lettura = Timestamp.valueOf("2020-11-01 00:00:00"), motivazione = null,
      data_caricamento = Timestamp.valueOf("2020-11-01 00:00:00"), annomese = "202011", flusso = VTG, data_calcolo = "2020-01-01",
      riempimento = 0, gruppo_flusso = GRUPPO_MISURE_VOLTURE, gruppo_periodo_competenza = "202011", gruppo_priorita = 1)

    val expectedRmv = FornitureMisureGasGruppoModel(codice_fiscale = "cf1", p_iva = "piva1", codice_pdr = "pdr1",
      codice_fornitura = "forn1", lettura = 1, data_lettura = Timestamp.valueOf("2020-11-02 00:00:00"), motivazione = "3",
      data_caricamento = Timestamp.valueOf("2020-11-02 00:00:00"), annomese = "202011", flusso = RMV, data_calcolo = "2020-01-01",
      riempimento = 0, gruppo_flusso = GRUPPO_MISURE_VOLTURE, gruppo_periodo_competenza = "202011",
      gruppo_priorita = 0)

    val result = misureGasFlow.calcolaPrioritaMisure(fornitureMisureGasDs, "2020-01-01").cache()

    Assert.assertEquals(expectedRml, result.where(col(FornitureMisureGasGruppoSchema.flusso) === RML).head())
    Assert.assertEquals(expectedTml, result.where(col(FornitureMisureGasGruppoSchema.flusso) === TML).head())
    Assert.assertEquals(expectedTgl, result.where(col(FornitureMisureGasGruppoSchema.flusso) === TGL).head())
    Assert.assertEquals(expectedRgl, result.where(col(FornitureMisureGasGruppoSchema.flusso) === RGL).head())
    Assert.assertEquals(expectedVtg, result.where(col(FornitureMisureGasGruppoSchema.flusso) === VTG).head())
    Assert.assertEquals(expectedRmv, result.where(col(FornitureMisureGasGruppoSchema.flusso) === RMV).head())
  }

  def testCalcolaMisureFinali(): Unit = {
    val fornitureMisureGas = Seq(
      // misura rettificata successivamente
      FornitureMisureGasGruppoModel(
        codice_fiscale = "cf1", codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 1,
        annomese = "202001", flusso = TAV, gruppo_flusso = GRUPPO_MISURE_AUTOLETTURE,
        gruppo_periodo_competenza = "202001", gruppo_priorita = 0
      ),
      FornitureMisureGasGruppoModel(
        codice_fiscale = "cf1", codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 2,
        annomese = "202002", flusso = TML, gruppo_flusso = GRUPPO_MISURE_MENSILI,
        gruppo_periodo_competenza = "202002", gruppo_priorita = 1
      ),
      FornitureMisureGasGruppoModel(
        codice_fiscale = "cf1", codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 3,
        annomese = "202002", flusso = RML, gruppo_flusso = GRUPPO_MISURE_MENSILI,
        gruppo_periodo_competenza = "202002", gruppo_priorita = 0
      ),
      FornitureMisureGasGruppoModel(
        codice_fiscale = "cf1", codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 5,
        annomese = "202003", flusso = RML, gruppo_flusso = GRUPPO_MISURE_MENSILI,
        gruppo_periodo_competenza = "202003", gruppo_priorita = 0
      ),
      // misura mensile da giornaliero
      FornitureMisureGasGruppoModel(
        codice_fiscale = "cf1", codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 8,
        annomese = "202004", flusso = TGL, gruppo_flusso = GRUPPO_MISURE_MENSILI,
        gruppo_periodo_competenza = "202004", gruppo_priorita = 8
      )
    ).toDS()

    val result = new MisureGasFlow().calcolaDelta(fornitureMisureGas, "2020-01-01").cache()

    Assert.assertEquals(5, result.count())
  }

  def testRaggruppaMisurePerFornitura(): Unit = {
    val fornitureMisureGas = Seq(
      FornitureMisureGasDeltaModel(
        codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 2, data_lettura = Timestamp.valueOf("2020-01-01 00:00:00"),
        annomese = "202001", categoria_misura = CATEGORIA_MISURE_MENSILI, tipo_misura = TIPO_LETTURA_PERIODICA, delta_misure = 0),
      FornitureMisureGasDeltaModel(
        codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 2, data_lettura = Timestamp.valueOf("2020-01-01 00:00:00"),
        annomese = "202001", categoria_misura = CATEGORIA_MISURE_AF, tipo_misura = TIPO_LETTURA_PERIODICA, delta_misure = 0),
      FornitureMisureGasDeltaModel(
        codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 2, data_lettura = Timestamp.valueOf("2021-02-01 00:00:00"),
        annomese = "202102", categoria_misura = CATEGORIA_MISURE_MENSILI, tipo_misura = TIPO_LETTURA_PERIODICA, delta_misure = 1),
      FornitureMisureGasDeltaModel(
        codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 3, data_lettura = Timestamp.valueOf("2021-03-01 00:00:00"),
        annomese = "202103", categoria_misura = CATEGORIA_MISURE_AUTOLETTURA, tipo_misura = TIPO_MISURE_AUTOLETTURA),
      FornitureMisureGasDeltaModel(codice_pdr = "pdr1", codice_fornitura = "forn1", lettura = 1,
        data_lettura = Timestamp.valueOf("2021-01-01 00:00:00"), annomese = "202101", categoria_misura = CATEGORIA_MISURE_VOLTURA,
        tipo_misura = TIPO_MISURE_VOLTURA)
    ).toDS()

    val expected = MisuraDettaglio(codice_fornitura = "forn1", pdr = "pdr1",
      autoletture = Array(
        AutoletturaDettaglio(competenza_consumi = "202103", data_lettura = "20210301", lettura_mese = "3.0",
          tipo_misura = TIPO_MISURE_AUTOLETTURA)
      ),
      misure_giornaliere = Array(),
      misure_altre_frequenze = Array(
        MensileDettaglio(competenza_consumi = "202001", data_lettura = "20200101", delta_misure = "0.0",
          lettura_mese = "2.0", tipo_misura = TIPO_LETTURA_PERIODICA)
      ),
      misure_mensili = Array(
        MensileDettaglio(competenza_consumi = "202001", data_lettura = "20200101", delta_misure = "0.0",
          lettura_mese = "2.0", tipo_misura = TIPO_LETTURA_PERIODICA),
        MensileDettaglio(competenza_consumi = "202102", data_lettura = "20210201", delta_misure = "1.0",
          lettura_mese = "2.0", tipo_misura = TIPO_LETTURA_PERIODICA)
      ),
      volture = Array(
        VolturaDettaglio(competenza_consumi = "202101", data_lettura = "20210101", lettura_misura = "1.0",
          tipo_misura = TIPO_MISURE_VOLTURA)
      )
    )

    val result = new MisureGasFlow().raggruppaMisurePerFornitura(fornitureMisureGas).cache()

    Assert.assertEquals(expected.autoletture(0), result.head().autoletture(0))
    Assert.assertEquals(expected.misure_giornaliere.length, result.head().misure_giornaliere.length)
    Assert.assertEquals(expected.misure_mensili(0), result.head().misure_mensili(0))
    Assert.assertEquals(expected.misure_mensili(1), result.head().misure_mensili(1))
    Assert.assertEquals(expected.volture(0), result.head().volture(0))
  }

  def testCalcolaMisureRiempimentoMensiliDaGiornalieri(): Unit = {
    val misureDs = Seq(
      FornitureMisureGasGruppoModel(codice_fornitura = "1", annomese = "202001", flusso = TAL),
      FornitureMisureGasGruppoModel(codice_fornitura = "1", annomese = "202001", flusso = TGL, lettura = 10),
      FornitureMisureGasGruppoModel(codice_fornitura = "2", annomese = "202001", flusso = TML),
      FornitureMisureGasGruppoModel(codice_fornitura = "2", annomese = "202002", flusso = RGL, lettura = 2, data_lettura = Timestamp.valueOf("2020-02-10 00:00:00")),
      FornitureMisureGasGruppoModel(codice_fornitura = "2", annomese = "202002", flusso = RGL, lettura = 6, data_lettura = Timestamp.valueOf("2020-02-28 00:00:00"))
    ).toDS()

    val expected = FornitureMisureGasGruppoModel(codice_fornitura = "2", annomese = "202002", flusso = RGL, lettura = 6, data_lettura = Timestamp.valueOf("2020-02-28 00:00:00"))
    val result = new MisureGasFlow().calcolaMisureRiempimentoMensiliDaGiornalieri(misureDs).cache()

    Assert.assertEquals(1, result.count())
    Assert.assertEquals(expected.codice_fornitura, result.head().codice_fornitura)
    Assert.assertEquals(expected.annomese, result.head().annomese)
    Assert.assertEquals(expected.lettura, result.head().lettura)
  }

  def testCalcolaMisureRiempimentoInizioFineFornitura(): Unit = {
    val misureDs = Seq(
      FornitureMisureGasGruppoModel(codice_fornitura = "1", annomese = "202301", flusso = TAL),
      FornitureMisureGasGruppoModel(codice_fornitura = "3", annomese = "202303", flusso = TML)
    ).toDS()
    val fornitureDs = Seq(
      FornitureProcessiGasModel(codice_fornitura = "1", data_inizio_fornitura = Timestamp.valueOf("2023-01-01 00:00:00"), data_fine_fornitura = Timestamp.valueOf("2023-03-31 00:00:00")),
      FornitureProcessiGasModel(codice_fornitura = "2", data_inizio_fornitura = Timestamp.valueOf("2023-04-01 00:00:00"), data_fine_fornitura = null),
      FornitureProcessiGasModel(codice_fornitura = "3", data_inizio_fornitura = Timestamp.valueOf("2023-02-01 00:00:00"), data_fine_fornitura = Timestamp.valueOf("2023-03-31 00:00:00"))
    ).toDS()

    val expected1Fine = FornitureMisureGasGruppoModel(codice_fornitura = "1", annomese = "202303", flusso = _MAN)
    val expected2Inizio = FornitureMisureGasGruppoModel(codice_fornitura = "2", annomese = "202304", flusso = _MAN)
    val expected3Inizio = FornitureMisureGasGruppoModel(codice_fornitura = "3", annomese = "202302", flusso = _MAN)

    val result = new MisureGasFlow().calcolaMisureRiempimentoInizioFineFornitura(misureDs, fornitureDs, "202001", "209912", Timestamp.valueOf("2020-01-01 00:00:00"))
      .cache()

    Assert.assertEquals(1, result.where(col(FornitureMisureGasGruppoSchema.codice_fornitura) === "1").count())
    Assert.assertEquals(expected1Fine.annomese, result.where(col(FornitureMisureGasGruppoSchema.codice_fornitura) === "1").head().annomese)
    Assert.assertEquals(1, result.where(col(FornitureMisureGasGruppoSchema.codice_fornitura) === "2").count())
    Assert.assertEquals(expected2Inizio.annomese, result.where(col(FornitureMisureGasGruppoSchema.codice_fornitura) === "2").head().annomese)
    Assert.assertEquals(1, result.where(col(FornitureMisureGasGruppoSchema.codice_fornitura) === "3").count())
    Assert.assertEquals(expected3Inizio.annomese, result.where(col(FornitureMisureGasGruppoSchema.codice_fornitura) === "3").head().annomese)
  }

  def testCategorizzaMisure(): Unit = {
    val misure = Seq(
      FornitureMisureGasArricchiteModel(codice_fornitura = "1", lettura = 1, flusso = TAL, gruppo_flusso = GRUPPO_MISURE_AUTOLETTURE, annomese = "202301"),
      FornitureMisureGasArricchiteModel(codice_fornitura = "1", lettura = 2, flusso = TML, gruppo_flusso = GRUPPO_MISURE_MENSILI, annomese = "202301"),
      FornitureMisureGasArricchiteModel(codice_fornitura = "1", lettura = 3, flusso = TML, gruppo_flusso = GRUPPO_MISURE_MENSILI, annomese = "202302"),

      FornitureMisureGasArricchiteModel(codice_fornitura = "2", lettura = 1, flusso = RMV, delta_misure = 5, gruppo_flusso = GRUPPO_MISURE_VOLTURE, annomese = "202301"),
      FornitureMisureGasArricchiteModel(codice_fornitura = "2", lettura = 2, flusso = TML, gruppo_flusso = GRUPPO_MISURE_MENSILI, annomese = "202301"),
      FornitureMisureGasArricchiteModel(codice_fornitura = "2", lettura = 4, flusso = TAV, gruppo_flusso = GRUPPO_MISURE_AUTOLETTURE, annomese = "202302")
    ).toDS()

    val result = new MisureGasFlow().categorizzaMisure(misure).cache()

    val result1 = result.where(col(FornitureMisureGasDeltaSchema.codice_fornitura) === "1").cache()
    val result2 = result.where(col(FornitureMisureGasDeltaSchema.codice_fornitura) === "2").cache()

    Assert.assertEquals(2, result1.where(col(FornitureMisureGasDeltaSchema.categoria_misura) === CATEGORIA_MISURE_MENSILI).count())
    Assert.assertEquals(2, result1.where(col(FornitureMisureGasDeltaSchema.categoria_misura) === CATEGORIA_MISURE_AF).count())
    Assert.assertEquals(2, result2.where(col(FornitureMisureGasDeltaSchema.categoria_misura) === CATEGORIA_MISURE_MENSILI).count())
  }

}
