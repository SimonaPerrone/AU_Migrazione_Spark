package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.schema.rcugas.{RcuGasFornituraPSchema, RcuGasSospensioniPSchema}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class SuspendedPdrDAOTest extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = new SuspendedPdrDAOMock().get().cache
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    result.collect.foreach(println)

    Assert.assertEquals(3, result.count)
    Assert.assertEquals(formatter.parseLocalDate("1959-08-01").toDateTimeAtStartOfDay, result.filter(_.nIdPdr.equals("3")).first().dataIniSosp)

  }

  class SuspendedPdrDAOMock extends SuspendedPdrDAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("1", "2016-12-01", null, "2020-12-31", "2021-12-31"),
        ("2", "2020-12-31", "2020-12-01", "2020-11-30", "2021-11-30"),
        ("3", "1959-08-01", "2999-12-31", null, "3000-12-31")
      ).toDF(
        RcuGasSospensioniPSchema.n_id_pdr,
        RcuGasSospensioniPSchema.d_data_inizio_sosp,
        RcuGasSospensioniPSchema.d_data_revoca_sosp,
        RcuGasFornituraPSchema.d_data_inizio,
        RcuGasFornituraPSchema.d_data_fine
      )
    }

  }
}
