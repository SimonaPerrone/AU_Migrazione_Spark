package it.eng.au.ccgPubblicazione.sessionFactory.impl

import it.eng.au.ccgPubblicazione.EnvironmentSparkTest
import it.eng.au.ccgPubblicazione.controller.sessionfactory.impl.SbgSession
import it.eng.au.ccgPubblicazione.model.agg.{DailyConsumptionAgg, ValidatedFlowAgg}
import it.eng.au.ccgPubblicazione.model.request.RequestFilter
import it.eng.au.ccgPubblicazione.schema.aggsbg.DailyConsumptionAggSchema
import it.eng.au.ccgPubblicazione.schema.rcugas.{RcugasPdrSchema, RcugasVarMisuratoreSchema}
import it.eng.au.ccgPubblicazione.utility.Constants._
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.commons.io.FileUtils

import java.io.File
import java.sql.Timestamp
import scala.util.Try

class SbgSessionTest extends EnvironmentSparkTest {
  Environment.setSessione("SBG")
  Environment.setDataRichiesta("2021-05-01")

  def testRunFilterUddIncoerenti(): Unit = {
    val spark = Environment.spark
    import spark.implicits._

    val consumption = List(
      DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "1", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "2", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-02 00:00:00"))
      , DailyConsumptionAgg(pdr = "1", pivaUdd = "123", annoMese = "202205", value = "3", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-03 00:00:00"))
      , DailyConsumptionAgg(pdr = "2", pivaUdd = "456", annoMese = "202205", value = "1", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "3", pivaUdd = "456", annoMese = "202205", value = "1", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "4", pivaUdd = "456", annoMese = "202205", value = "1", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-01 00:00:00"))
      , DailyConsumptionAgg(pdr = "5", pivaUdd = "111", annoMese = "202205", value = "1", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-01 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "5", pivaUdd = "111", annoMese = "202205", value = "100001", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-02 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "5", pivaUdd = "111", annoMese = "202205", value = "1", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-03 00:00:00"), ca = "0.1")
      , DailyConsumptionAgg(pdr = "6", pivaUdd = "111", annoMese = "202205", value = "1", valuef3 = "1.5", classeMisuratore = "G4", date = Timestamp.valueOf("2022-05-03 00:00:00"), ca = "0.1", valueNotSterilized = "123")
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

    val rcugasPdr = Seq(
      ("nIdPdr1", "1"),
      ("nIdPdr2", "2"),
      ("nIdPdr3", "3"),
      ("nIdPdr4", "4"),
      ("nIdPdr5", "5"),
      ("nIdPdr6", "6")
    ).toDF(RcugasPdrSchema.getValues: _*)
    val rcugasPdrTableName = Environment.getRcugasPdrTableName

    val rcugasVarMisuratore = Seq(
      ("nIdPdr1", 1.5, "2022-05-01 00:00:00", "2022-06-01 00:00:00"),
      ("nIdPdr2", 1.5, "2022-05-01 00:00:00", "2022-06-01 00:00:00"),
      ("nIdPdr3", 1.5, "2022-05-01 00:00:00", "2022-06-01 00:00:00"),
      ("nIdPdr4", 1.5, "2022-05-01 00:00:00", "2022-06-01 00:00:00"),
      ("nIdPdr5", 1.5, "2022-05-01 00:00:00", "2022-06-01 00:00:00"),
      ("nIdPdr6", 1.5, "2022-05-01 00:00:00", "2022-06-01 00:00:00")
    ).toDF(RcugasVarMisuratoreSchema.getValues: _*)
    val rcugasVarMisuratoreTableName = Environment.getRcugasVarMisuratoreTableName

    Try(FileUtils.cleanDirectory(new File("src/test/resources/hdfs/rcugas_pdr_p")))
    Try(FileUtils.cleanDirectory(new File("src/test/resources/hdfs/rcugas_var_misuratore_p")))
    Environment.sqlContext.sql("drop database if exists test cascade")
    Environment.sqlContext.sql("create database if not exists test")
    Environment.sqlContext.sql(s"create external table $rcugasPdrTableName (n_id_pdr string, t_codice_pdr string) location 'src/test/resources/hdfs/rcugas_pdr_p'")
    Environment.sqlContext.sql(s"create external table $rcugasVarMisuratoreTableName (n_id_pdr string, n_coeff_correzione double, d_data_inizio timestamp, d_data_fine timestamp) location 'src/test/resources/hdfs/rcugas_var_misuratore_p'")
    rcugasPdr.write.insertInto(rcugasPdrTableName)
    rcugasVarMisuratore.write.insertInto(rcugasVarMisuratoreTableName)

    SbgSession.runRunnableAggregator(consumption, validation, request, UDD, FILTRO)
  }
}
