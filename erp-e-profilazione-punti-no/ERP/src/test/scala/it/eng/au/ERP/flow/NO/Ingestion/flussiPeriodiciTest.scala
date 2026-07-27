package it.eng.au.ERP.flow.NO.Ingestion

import it.eng.au.ERP.EnvironmentSparkTest
import it.eng.au.ERP.model.au.{flussoMisureNoAggr1Model, flussoMisureNoAggr4Model}
import it.eng.au.ERP.model.tratt_pod.TrattPodAnnomesePartitionedModel
import it.eng.au.ERP.schema.au.flussoMisureNoAggrSchema
import it.eng.au.ERP.trasformations.NO.{CalcoloPrelevatoPuntiPrelievoMisNOTrasformation, CalcoloPrelevatoPuntiPrelievoNonOrari}
import it.eng.au.ERP.utility.environment.Environment
import it.eng.au.ERP.utility.functions.argumentsUtilities
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import org.junit.Test


class flussiPeriodiciTest  extends EnvironmentSparkTest {
  implicit val spark = Environment.getSpark

  import spark.implicits._

  val joinModel = "joinModel"
  val year = Some(2025)
  val month = Some(7)
  val podExcluded = List("901")
  val executionId = System.currentTimeMillis()

  val annomese = Some("202507")
  val start_in = argumentsUtilities.startInFunction(annomese)
  val stop_in = argumentsUtilities.stopInFunction(annomese)


  def flussiPeriodiciSegmentoSinistro(): DataFrame = {
    import spark.implicits._

    // Creo DataFrame con tutti i campi necessari, incluso data_misura_yyyymmdd, validato, tipodato_e
    val dfFlussoMisureNoAggr = Seq(
      ("12345678901", "A123", "Area1", 1, "UC001", "901", "98765432109", "2025-01-01", "20250101", 230.0, 10.5, 11.0, 2025, 6, "RNO", "S", 1),
      ("11111111111", "B456", "Area2", 0, "UC002", "123", "12312312312", "2025-01-02", "20250102", 220.0, 15.2, 14.8, 2025, 7, "RNO", "S", 1),
      ("22222222222", "C789", "Area3", 0, "UC003", "345", "33333333333", "2025-06-30", "20250630", 240.0, 20.0, 18.5, 2025, 6, "PNO", "S", 1),
      ("33333333333", "D012", "Area4", 0, "UC004", "678", "44444444444", "2025-06-30", "20250630", 250.0, 25.0, 22.0, 2025, 6, "PNO2G", "S", 1)
    ).toDF(
      "pivadistributore", "codcontrdisp", "area", "isnew_flusso", "coduc", "pod",
      "pivautente", "data_misura", "data_misura_yyyymmdd", "tensione", "e1", "e2",
      "anno", "mese", "tipo_flusso", "validato", "tipodato_e"
    )

    val finalDf = CalcoloPrelevatoPuntiPrelievoNonOrari.flussiPeriodiciSegmentoSinistro(
      dfFlussoMisureNoAggr,
      year, month, podExcluded
    )
    finalDf.select(col("pod"), col("tipo_flusso"), col("data_misura")).show()

    finalDf
  }

  def flussiPeriodiciSegmentoDestro(): DataFrame = {
    import spark.implicits._

    // Creo DataFrame con tutti i campi necessari, incluso data_misura_yyyymmdd, validato, tipodato_e
    val dfFlussoMisureNoAggr = Seq(
      ("12345678901", "A123", "Area1", 1, "UC001", "901", "98765432109", "2025-01-01", "20250101", 230.0, 10.5, 11.0, 2025, 6, "RNO", "S", 1),
      ("11111111111", "B456", "Area2", 0, "UC002", "123", "12312312312", "2025-01-02", "20250102", 220.0, 15.2, 14.8, 2025, 7, "RNO", "S", 1),
      ("22222222222", "C789", "Area3", 0, "UC003", "345", "33333333333", "2025-07-31", "20250731", 240.0, 20.0, 18.5, 2025, 7, "RNO", "S", 1),
      ("33333333333", "D012", "Area4", 0, "UC004", "678", "44444444444", "2025-06-30", "20250630", 250.0, 25.0, 22.0, 2025, 7, "PNO2G", "S", 1)
    ).toDF(
      "pivadistributore", "codcontrdisp", "area", "isnew_flusso", "coduc", "pod",
      "pivautente", "data_misura", "data_misura_yyyymmdd", "tensione", "e1", "e2",
      "anno", "mese", "tipo_flusso", "validato", "tipodato_e"
    )

    val finalDf = CalcoloPrelevatoPuntiPrelievoNonOrari.flussiPeriodiciSegmentoDestro(
      dfFlussoMisureNoAggr,
      year, month, podExcluded
    )
    finalDf.select(col("pod"), col("tipo_flusso"), col("data_misura")).show()

    finalDf
  }

  def flussiPeriodiciSegmentoSinistraDestro(): DataFrame = {
    import spark.implicits._

    // Creo DataFrame con tutti i campi necessari, incluso data_misura_yyyymmdd, validato, tipodato_e
    val dfFlussoMisureNoAggr = Seq(
      ("12345678901", "A123", "Area1", 1, "UC001", "901", "98765432109", "2025-01-01", "20250101", 230.0, 10.5, 11.0, 2025, 6, "RNO", "S", 1),
      ("11111111111", "B456", "Area2", 0, "UC002", "123", "12312312312", "2025-01-02", "20250102", 220.0, 15.2, 14.8, 2025, 7, "RNO", "S", 1),
      ("22222222222", "C789", "Area3", 0, "UC003", "345", "33333333333", "2025-06-30", "20250630", 240.0, 20.0, 18.5, 2025, 6, "PNO", "S", 1),
      ("33333333333", "D012", "Area4", 0, "UC004", "678", "44444444444", "2025-06-30", "20250630", 250.0, 25.0, 22.0, 2025, 7, "PNO2G", "S", 1)
    ).toDF(
      "pivadistributore", "codcontrdisp", "area", "isnew_flusso", "coduc", "pod",
      "pivautente", "data_misura", "data_misura_yyyymmdd", "tensione", "e1", "e2",
      "anno", "mese", "tipo_flusso", "validato", "tipodato_e"
    )

    val finalDf = CalcoloPrelevatoPuntiPrelievoNonOrari.flussiPeriodiciSegmentoSinistraEDestro(
      dfFlussoMisureNoAggr,
      year, month, podExcluded
    )
    finalDf.select(col("pod"), col("tipo_flusso"), col("data_misura")).show()

    finalDf
  }

  def trattPodllAnnomesePartitionedPreparedSegmentoSinistro(): Dataset[TrattPodAnnomesePartitionedModel] = {
    //trattPodllAnnomesePartitionedPreparedSegmentoSinistro
    val dfTrattPodAnnomesePartitioned = Seq(
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "F",
        stato_pod = "ATTIVO",
        pod14 = "901",
        anno = 2025,
        mese = 6
      ),
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "M",
        stato_pod = "ATTIVO",
        pod14 = "345",
        anno = 2025,
        mese = 6
      ),
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "Y",
        stato_pod = "ATTIVO",
        pod14 = "345",
        anno = 2025,
        mese = 6
      ),
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "C",
        stato_pod = "ATTIVO",
        pod14 = "123",
        anno = 2025,
        mese = 7
      )
    ).toDS

    val finalDf = CalcoloPrelevatoPuntiPrelievoNonOrari.trattPodllAnnomesePartitionedPreparedSegmentoSinistro(
      dfTrattPodAnnomesePartitioned,
      year, month, podExcluded
    )

    finalDf.show()

    finalDf
  }


  def trattPodllAnnomesePartitionedPreparedSegmentoDestro(): Dataset[TrattPodAnnomesePartitionedModel] = {
    val dfTrattPodAnnomesePartitioned = Seq(
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "F",
        stato_pod = "ATTIVO",
        pod14 = "901",
        anno = 2025,
        mese = 7
      ),
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "M",
        stato_pod = "ATTIVO",
        pod14 = "345",
        anno = 2025,
        mese = 7
      ),
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "Y",
        stato_pod = "ATTIVO",
        pod14 = "345",
        anno = 2025,
        mese = 7
      ),
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "C",
        stato_pod = "ATTIVO",
        pod14 = "123",
        anno = 2025,
        mese = 6
      )
    ).toDS

    val finalDf = CalcoloPrelevatoPuntiPrelievoNonOrari.trattPodllAnnomesePartitionedPreparedSegmentoDestro(
      dfTrattPodAnnomesePartitioned,
      year, month, podExcluded
    )

    finalDf.show()

    finalDf
  }


  def trattPodllAnnomesePartitionedPreparedSegmentoSinistraDestro(): Dataset[TrattPodAnnomesePartitionedModel] = {
    val dfTrattPodAnnomesePartitioned = Seq(
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "F",
        stato_pod = "ATTIVO",
        pod14 = "901",
        anno = 2025,
        mese = 7
      ),
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "M",
        stato_pod = "ATTIVO",
        pod14 = "345",
        anno = 2025,
        mese = 7
      ),
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "Y",
        stato_pod = "ATTIVO",
        pod14 = "345",
        anno = 2025,
        mese = 7
      ),
      TrattPodAnnomesePartitionedModel(
        is_t_trattamento = "C",
        stato_pod = "ATTIVO",
        pod14 = "123",
        anno = 2025,
        mese = 6
      )
    ).toDS

    val finalDf = CalcoloPrelevatoPuntiPrelievoNonOrari.trattPodllAnnomesePartitionedPreparedSegmentoSinistroEDestro(
      dfTrattPodAnnomesePartitioned,
      year, month, podExcluded
    )

    finalDf.show()

    finalDf
  }

  @Test
  def testFlussiPeriodiciSegmentoSinistro(): Unit = {
    val finalDf = flussiPeriodiciSegmentoSinistro()
    finalDf.show()
  }

  @Test
  def testFlussiPeriodiciSegmentoDestro() :Unit = {
    val finalDf =flussiPeriodiciSegmentoDestro()
    finalDf.show()
  }

  @Test
  def testFlussiPeriodiciSegmentoSinistraDestro():Unit = {
    val finalDf = flussiPeriodiciSegmentoSinistraDestro()
    finalDf.show()
  }

  @Test
  def testTrattPodllAnnomesePartitionedPreparedSegmentoSinistro(): Unit = {
    val finalDf = trattPodllAnnomesePartitionedPreparedSegmentoSinistro()
    finalDf.show()
  }

  @Test
  def testTrattPodllAnnomesePartitionedPreparedSegmentoDestro():Unit ={
    val finalDf = trattPodllAnnomesePartitionedPreparedSegmentoDestro()
    finalDf.show()
  }

  @Test
  def testTrattPodllAnnomesePartitionedPreparedSegmentoSinistraDestro():Unit = {
    val finalDf = trattPodllAnnomesePartitionedPreparedSegmentoSinistraDestro()
    finalDf.show()
  }

  @Test
  def testFlussiPeriodiciSpondaSinistra() :Unit = {
    val flussoMisureNoAff = flussiPeriodiciSegmentoSinistro()
    val trattPodAnnomesePartitioned = trattPodllAnnomesePartitionedPreparedSegmentoSinistro()
    val trattPodDf = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.prepareTrattPodForJoin(trattPodAnnomesePartitioned)
    val finalDf = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.calcoloPrelevatoPuntiPrelievoNOIngestionFlussiPeriodiciSpondaSinistraSegmentoFlusso1(flussoMisureNoAff
      ,trattPodDf,start_in,stop_in,executionId
    )

    finalDf.show()
  }

  @Test
  def testFlussiPeriodiciSpondaDestra() :Unit = {
    val flussoMisureNoAff = flussiPeriodiciSegmentoDestro()
    val trattPodAnnomesePartitioned = trattPodllAnnomesePartitionedPreparedSegmentoDestro()
    val trattPodDf = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.prepareTrattPodForJoin(trattPodAnnomesePartitioned)
    val finalDf = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.calcoloPrelevatoPuntiPrelievoNOIngestionFlussiPeriodiciSpondaDestraSegmentoFlusso1(flussoMisureNoAff
      ,trattPodDf,start_in,stop_in,executionId
    )
    finalDf.show()

  }

  @Test
  def testFlussiPeriodiciSpondaSinistraDestra() :Unit = {
    val flussoMisureNoAff = flussiPeriodiciSegmentoSinistraDestro()
    val trattPodAnnomesePartitioned = trattPodllAnnomesePartitionedPreparedSegmentoSinistraDestro()
    val trattPodDf = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.prepareTrattPodForJoin(trattPodAnnomesePartitioned)

    val finalDf = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.calcoloPrelevatoPuntiPrelievoNOIngestionFlussiPeriodiciFlusso2(flussoMisureNoAff
      ,trattPodDf,start_in,stop_in,executionId
    )
    finalDf.show()

  }

}
