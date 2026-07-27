package it.eng.au.ccgPubblicazione.sessionFactory.impl

import it.eng.au.ccgPubblicazione.EnvironmentSparkTest
import it.eng.au.ccgPubblicazione.controller.runnablefile.impl.agg.aggregato.AggUddAggregato
import it.eng.au.ccgPubblicazione.controller.sessionfactory.impl.AggSession
import it.eng.au.ccgPubblicazione.controller.sessionfactory.impl.AggSession.uddPdrElencoFlussi
import it.eng.au.ccgPubblicazione.model.agg.{DailyConsumptionAgg, ValidatedFlowAgg}
import it.eng.au.ccgPubblicazione.model.request.{RequestFilter, RequestPdr}
import it.eng.au.ccgPubblicazione.schema.aggsbg.DailyConsumptionAggSchema
import it.eng.au.ccgPubblicazione.utility.Constants.{AMMISSIBILITA_FILE, AMMISSIBILITA_PDR, FILTRO, GESTORE, AMMISSIBILITA_NO_0, PDR, UDD, incoerentiAB, incoerentiC}
import it.eng.au.ccgPubblicazione.utility.FileUtility.putIntoZip
import it.eng.au.ccgPubblicazione.utility.{Environment, FileUtility}

import java.io.{File, FileOutputStream}
import java.sql.Timestamp
import java.util.zip.ZipOutputStream
import scala.util.matching.Regex

class AggSessionTest extends EnvironmentSparkTest{
Environment.setSessione("AGG")
Environment.setDataRichiesta("2021-05-01")

  def testRunPdrUdd(): Unit = {
    val spark = Environment.spark

    import spark.implicits._

    val consumption = List(
      DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "2", date = Timestamp.valueOf("2022-05-02 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "3", date = Timestamp.valueOf("2022-05-03 00:00:00"))
      , DailyConsumptionAgg(pdr = "2", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "3", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "4", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF
      .selectExpr(DailyConsumptionAggSchema.getValues: _*)

    val validation = List(
      ValidatedFlowAgg(pdr = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "2", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "3", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF

    val request = List(
      RequestPdr(N_ID_RICHIESTA = "1", T_PIVA = "123", T_RUOLO = UDD, T_CODICE_PDR = "1")
      , RequestPdr(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "2")
      , RequestPdr(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "3", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "1")
      , RequestPdr(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "5", T_PIVA = "789", T_RUOLO = UDD, T_CODICE_PDR = "10")
      , RequestPdr(N_ID_RICHIESTA = "6", T_PIVA = "111", T_RUOLO = UDD, T_CODICE_PDR = "1", B_AMMISSIBILITA = AMMISSIBILITA_NO_0, T_TIPO_AMM = AMMISSIBILITA_PDR)
      , RequestPdr(N_ID_RICHIESTA = "7", T_PIVA = "111", T_RUOLO = UDD, T_CODICE_PDR = "1", B_AMMISSIBILITA = AMMISSIBILITA_NO_0, T_TIPO_AMM = AMMISSIBILITA_FILE)
      , RequestPdr(N_ID_RICHIESTA = "8", T_PIVA = "111", T_RUOLO = UDD, T_CODICE_PDR = "1", B_AMMISSIBILITA = AMMISSIBILITA_NO_0, T_TIPO_AMM = AMMISSIBILITA_FILE)
    ).toDF

    AggSession.runRunnableAggregator(consumption, validation, request, UDD, PDR)

  }


  def testRunFilterUdd(): Unit = {
    val spark = Environment.spark

    import spark.implicits._

    val consumption = List(
      DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "2", date = Timestamp.valueOf("2022-05-02 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "3", date = Timestamp.valueOf("2022-05-03 00:00:00"))
      , DailyConsumptionAgg(pdr = "2", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "3", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "4", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF
      .selectExpr(DailyConsumptionAggSchema.getValues: _*)

    val validation = List(
      ValidatedFlowAgg(pdr = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "2", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "3", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF

    val request = List(
      RequestFilter(N_ID_RICHIESTA = "1", T_PIVA = "123", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "3", T_PIVA = "456", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD)
    ).toDF

    AggSession.runRunnableAggregator(consumption, validation, request, UDD, FILTRO)

  }


  def testRunPdrGestore(): Unit = {
    val spark = Environment.spark

    import spark.implicits._

    val consumption = List(
      DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "2", date = Timestamp.valueOf("2022-05-02 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "3", date = Timestamp.valueOf("2022-05-03 00:00:00"))
      , DailyConsumptionAgg(pdr = "2", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "3", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "4", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF
      .selectExpr(DailyConsumptionAggSchema.getValues: _*)

    val validation = List(
      ValidatedFlowAgg(pdr = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "2", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "3", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF

    val request = List(
      RequestPdr(N_ID_RICHIESTA = "1", T_PIVA = "111", T_RUOLO = GESTORE, T_CODICE_PDR = "1")
      , RequestPdr(N_ID_RICHIESTA = "2", T_PIVA = "444", T_RUOLO = GESTORE, T_CODICE_PDR = "2")
      , RequestPdr(N_ID_RICHIESTA = "2", T_PIVA = "444", T_RUOLO = GESTORE, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "3", T_PIVA = "444", T_RUOLO = GESTORE, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "4", T_PIVA = "444", T_RUOLO = GESTORE, T_CODICE_PDR = "1")
      , RequestPdr(N_ID_RICHIESTA = "4", T_PIVA = "444", T_RUOLO = GESTORE, T_CODICE_PDR = "3")
    ).toDF

    AggSession.runRunnableAggregator(consumption, validation, request, GESTORE, PDR)

  }

  def testRunFilterUddIncoerenti(): Unit = {
    val spark = Environment.spark

    import spark.implicits._

    val consumption = List(
      DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "2", date = Timestamp.valueOf("2022-05-02 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "3", date = Timestamp.valueOf("2022-05-03 00:00:00"))
      , DailyConsumptionAgg(pdr = "2", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "3", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "4", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "5", pivaUdd = "111", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "5", pivaUdd = "111", annoMese = "202205", value = "100001", date = Timestamp.valueOf("2022-05-02 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "5", pivaUdd = "111", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-03 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "6", pivaUdd = "111", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-03 00:00:00"), ca = "0.1", valueNotSterilized = "123")
    ).toDF
      .selectExpr(DailyConsumptionAggSchema.getValues: _*)

    val validation = List(
      ValidatedFlowAgg(pdr = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "2", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "3", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF

    val request = List(
      RequestFilter(N_ID_RICHIESTA = "1", T_PIVA = "123", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "3", T_PIVA = "456", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD)
      , RequestFilter(N_ID_RICHIESTA = "5", T_PIVA = "111", T_RUOLO = UDD, T_INCOERENTI = incoerentiAB)
      , RequestFilter(N_ID_RICHIESTA = "6", T_PIVA = "111", T_RUOLO = UDD, T_INCOERENTI = incoerentiC)
    ).toDF

    AggSession.runRunnableAggregator(consumption, validation, request, UDD, FILTRO)

  }

  def testRunPdrUddNoConsumption(): Unit = {
    val spark = Environment.spark

    import spark.implicits._

    val consumption = List(
      DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "2", date = Timestamp.valueOf("2022-05-02 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "3", date = Timestamp.valueOf("2022-05-03 00:00:00"))
      , DailyConsumptionAgg(pdr = "2", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "3", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "4", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF
      .selectExpr(DailyConsumptionAggSchema.getValues: _*)

    val validation = List(
      ValidatedFlowAgg(pdr = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "2", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "3", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF

    val request = List(
      RequestPdr(N_ID_RICHIESTA = "1", T_PIVA = "123", T_RUOLO = UDD, T_CODICE_PDR = "1")
      , RequestPdr(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "2")
      , RequestPdr(N_ID_RICHIESTA = "2", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "3", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "1")
      , RequestPdr(N_ID_RICHIESTA = "4", T_PIVA = "456", T_RUOLO = UDD, T_CODICE_PDR = "3")
      , RequestPdr(N_ID_RICHIESTA = "5", T_PIVA = "789", T_RUOLO = UDD, T_CODICE_PDR = "10")
      , RequestPdr(N_ID_RICHIESTA = "6", T_PIVA = "111", T_RUOLO = UDD, T_CODICE_PDR = "1", B_AMMISSIBILITA = AMMISSIBILITA_NO_0, T_TIPO_AMM = AMMISSIBILITA_PDR)
      , RequestPdr(N_ID_RICHIESTA = "7", T_PIVA = "111", T_RUOLO = UDD, T_CODICE_PDR = "1", B_AMMISSIBILITA = AMMISSIBILITA_NO_0, T_TIPO_AMM = AMMISSIBILITA_FILE)
      , RequestPdr(N_ID_RICHIESTA = "8", T_PIVA = "111", T_RUOLO = UDD, T_CODICE_PDR = "1", B_AMMISSIBILITA = AMMISSIBILITA_NO_0, T_TIPO_AMM = AMMISSIBILITA_FILE)
      , RequestPdr(N_ID_RICHIESTA = "9", T_PIVA = "123", T_RUOLO = UDD, T_CODICE_PDR = "1", T_MESE = "05")

    ).toDF

    AggSession.runRunnableAggregator(consumption, validation, request, UDD, PDR)
  }

  def testRunPdrGestoreIncoerenti(): Unit = {
    val spark = Environment.spark

    import spark.implicits._

    val consumption = List(
      DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "2", date = Timestamp.valueOf("2022-05-02 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "3", date = Timestamp.valueOf("2022-05-03 00:00:00"))
      , DailyConsumptionAgg(pdr = "2", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "3", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "4", pivaUdd = "456", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "5", pivaUdd = "211", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "5", pivaUdd = "211", annoMese = "202205", value = "100001", date = Timestamp.valueOf("2022-05-02 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "5", pivaUdd = "211", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-03 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "6", pivaUdd = "311", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "6", pivaUdd = "311", annoMese = "202205", value = "100001", date = Timestamp.valueOf("2022-05-02 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "6", pivaUdd = "311", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-03 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "7", pivaUdd = "411", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "7", pivaUdd = "411", annoMese = "202205", value = "100001", date = Timestamp.valueOf("2022-05-02 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "7", pivaUdd = "411", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-03 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "8", pivaUdd = "111", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "8", pivaUdd = "111", annoMese = "202205", value = "100001", date = Timestamp.valueOf("2022-05-02 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "8", pivaUdd = "111", annoMese = "202205", value = "1", date = Timestamp.valueOf("2022-05-03 00:00:00"), ca = "0.1")
    ).toDF
      .selectExpr(DailyConsumptionAggSchema.getValues: _*)

    val validation = List(
      ValidatedFlowAgg(pdr = "1", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "2", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "3", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "5", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "6", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "7", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , ValidatedFlowAgg(pdr = "8", date = Timestamp.valueOf("2022-05-01 00:00:00"))
    ).toDF

    val request = List(
      RequestFilter(N_ID_RICHIESTA = "1", T_PIVA = "111", T_RUOLO = GESTORE, T_INCOERENTI = incoerentiAB)
    ).toDF

    AggSession.runRunnableAggregator(consumption, validation, request, GESTORE, FILTRO)

  }
}
