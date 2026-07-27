package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasVarProfiloPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import junit.framework.TestCase
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class RcuGasVarProfiloPDAOTest extends TestCase with EnvironmentSparkTest {

  def testRead(): Unit = {

    val rcuList = (new RcuGasVarProfiloPDAOMock).get("202111", "202201").cache()

    rcuList.collect.foreach(println)
    Assert.assertEquals(2, rcuList.count())

  }

  class RcuGasVarProfiloPDAOMock extends RcuGasVarProfiloPDAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("", null, null, "CX02")
        , ("", "2022-01-01 00:00:00.0", "2022-01-01 00:00:00.0", "CX02")
      ).toDF(
        RcuGasVarProfiloPSchema.n_id_pdr,
        RcuGasVarProfiloPSchema.d_data_inizio,
        RcuGasVarProfiloPSchema.d_data_fine,
        RcuGasVarProfiloPSchema.t_cod_profilo
      )
    }
  }

}
