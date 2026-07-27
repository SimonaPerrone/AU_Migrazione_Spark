package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasVarPrelAnnuoPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import junit.framework.TestCase
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class RcuGasVarPrelAnnuoPDAOTest extends TestCase with EnvironmentSparkTest {

  def testRead(): Unit = {

    val rcuList = (new RcuGasVarPrelAnnuoPDAOMock).get("202111", "202201").cache()

    rcuList.collect.foreach(println)
    Assert.assertEquals(2, rcuList.count())

  }
  class RcuGasVarPrelAnnuoPDAOMock extends RcuGasVarPrelAnnuoPDAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("", null, null, "10")
        , ("", "2022-01-01 00:00:00.0", "2022-01-01 00:00:00.0", "10")
      ).toDF(
        RcuGasVarPrelAnnuoPSchema.n_id_pdr,
        RcuGasVarPrelAnnuoPSchema.d_data_inizio,
        RcuGasVarPrelAnnuoPSchema.d_data_fine,
        RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo
      )
    }
  }
}
