package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasConnessioniDistr2Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import junit.framework.TestCase
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class RcuGasConnessioniDistr2DAOTest extends TestCase with EnvironmentSparkTest {

  def testRead(): Unit = {

    val rcuList = (new RcuGasConnessioniDistr2DAOMock).get("202201", "202203").cache()

    rcuList.collect.foreach(println)
    Assert.assertEquals(3, rcuList.count())
  }
  class RcuGasConnessioniDistr2DAOMock extends RcuGasConnessioniDistr2DAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("1", "", "", null, null, null, null, "1")
        , ("2", "", "", "2022-01-01 00:00:00.0", "2022-01-01 00:00:00.0", "2022-01-01 00:00:00.0", "2022-01-01 00:00:00.0", "1")
        , ("3", "", "", "2022-01-01 00:00:00.0", "2022-01-01 00:00:00.0", "2022-04-01 00:00:00.0", "2022-05-01 00:00:00.0", "1")
        , ("4", "", "", "2022-01-01 00:00:00.0", "2022-04-01 00:00:00.0", "2022-03-01 00:00:00.0", "2022-05-01 00:00:00.0", "1")
      ).toDF(
        RcuGasConnessioniDistr2Schema.t_codice_pdr,
        RcuGasConnessioniDistr2Schema.n_id_distr,
        RcuGasConnessioniDistr2Schema.t_remi,
        RcuGasConnessioniDistr2Schema.d_data_inizio_conn,
        RcuGasConnessioniDistr2Schema.d_data_fine_conn,
        RcuGasConnessioniDistr2Schema.d_data_inizio_aggregazione,
        RcuGasConnessioniDistr2Schema.d_data_fine_aggregazione,
        RcuGasConnessioniDistr2Schema.id_regione_climatica
      )
    }
  }
}
