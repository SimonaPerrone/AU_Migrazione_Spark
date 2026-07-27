package it.eng.au.aggiustamentoGas.dao.settlegas

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.schema.settlegas.TabProfiliGiorniStdPercSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import org.junit.Assert

class TabProfiliGiorniStdPercDaoTestEnvironment extends EnvironmentSparkTest {
 /* def testGet(): Unit = {

    val startDate = "201801"
    val endDate = "202201"

    val result = new TabProfiliGiorniStdPercDaoMock().get(startDate, endDate).cache()

    result.collect().foreach(println)
    Assert.assertEquals(2, result.count())
    Assert.assertEquals(new DateTime().withDate(2018, 1, 1).withTimeAtStartOfDay().toString("yyyyMMdd"), result.first().data)
    Assert.assertEquals("T2A2", result.first().prof)
    Assert.assertEquals(24, result.first().idRegClim)
    Assert.assertEquals(9.652445387842567E-9, result.first().pprofkPercentage, 0.00001)
  }

  class TabProfiliGiorniStdPercDaoMock extends TabProfiliGiorniStdPercDao {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._
      List(
        ("01/01/2018", "T2A2", 24, 9.652445387842567e-9)
        , ("31/12/2017", "T2A2", 24, 9.652445387842567e-9)
        , ("31/01/2022", "T2A2", 24, 9.652445387842567e-9)
        , ("01/02/2022", "T2A2", 24, 9.652445387842567e-9)
      ).toDF(
        TabProfiliGiorniStdPercSchema.data,
        TabProfiliGiorniStdPercSchema.prof,
        TabProfiliGiorniStdPercSchema.id_reg_clim,
        TabProfiliGiorniStdPercSchema.pprofk
      )
    }
  }*/
}


