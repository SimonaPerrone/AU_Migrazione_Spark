package it.eng.au.mid.dao

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao.RcugasConnessioniDistr2RemiPDaoMock
import it.eng.au.mid.model.hive.rcugas.RcugasConnessioniDistr2RemiPModel
import it.eng.au.mid.schema.hive.rcugas.RcugasConnessioniDistr2RemiPSchema
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class RcugasConnessioniDistr2RemiPDaoTest extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  def testReadConnessioniAttive(): Unit = {
    val dataCalcolo = Timestamp.valueOf("2024-01-01 00:00:00")
    val connessioni = Seq(
      // ok
      RcugasConnessioniDistr2RemiPModel(
        t_codice_pdr = "ok1",
        d_data_inizio_conn = null,
        d_data_fine_conn = null,
        d_data_inizio_aggregazione = null,
        d_data_fine_aggregazione = null
      ),
      RcugasConnessioniDistr2RemiPModel(
        t_codice_pdr = "ok2",
        d_data_inizio_conn = Timestamp.valueOf("2024-01-01 00:00:00"),
        d_data_fine_conn = null,
        d_data_inizio_aggregazione = Timestamp.valueOf("2023-12-31 00:00:00"),
        d_data_fine_aggregazione = null
      ),
      // KO
      RcugasConnessioniDistr2RemiPModel(
        t_codice_pdr = "ko1",
        d_data_inizio_conn = Timestamp.valueOf("2024-01-02 00:00:00"),
        d_data_fine_conn = null,
        d_data_inizio_aggregazione = Timestamp.valueOf("2023-12-31 00:00:00"),
        d_data_fine_aggregazione = null
      ),
      RcugasConnessioniDistr2RemiPModel(
        t_codice_pdr = "ko2",
        d_data_inizio_conn = Timestamp.valueOf("2024-01-01 00:00:00"),
        d_data_fine_conn = null,
        d_data_inizio_aggregazione = Timestamp.valueOf("2024-01-02 00:00:00"),
        d_data_fine_aggregazione = null
      ),
      RcugasConnessioniDistr2RemiPModel(
        t_codice_pdr = "ko3",
        d_data_inizio_conn = Timestamp.valueOf("2022-01-01 00:00:00"),
        d_data_fine_conn = Timestamp.valueOf("2023-12-31 23:59:59"),
        d_data_inizio_aggregazione = Timestamp.valueOf("2024-01-01 00:00:00"),
        d_data_fine_aggregazione = null
      ),
      RcugasConnessioniDistr2RemiPModel(
        t_codice_pdr = "ko4",
        d_data_inizio_conn = Timestamp.valueOf("2022-01-01 00:00:00"),
        d_data_fine_conn = null,
        d_data_inizio_aggregazione = Timestamp.valueOf("2022-01-02 00:00:00"),
        d_data_fine_aggregazione = Timestamp.valueOf("2023-12-31 23:59:59")
      )
    ).toDS()

    val result = new RcugasConnessioniDistr2RemiPDaoMock(connessioni).readConnessioniAttive(dataCalcolo).cache()

    result.show()
    Assert.assertEquals(2, result.where(col(RcugasConnessioniDistr2RemiPSchema.t_codice_pdr).like("ok%")).count())
    Assert.assertEquals(0, result.where(col(RcugasConnessioniDistr2RemiPSchema.t_codice_pdr).like("ko%")).count())
  }
}
