package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasSospensioniPSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.Assert

class RcugasSospensioniReadTest extends EnvironmentSparkTest {
  def testReadRcugasSospensioni(): Unit = {
    Environment.setProperty("year.month", "202210")

    val rcugasSospensioniPDAOMock = new RcugasSospensioniPDAOMock
    val rcugasSospensioni = rcugasSospensioniPDAOMock.get
    rcugasSospensioni.show

    Assert.assertEquals(3, rcugasSospensioni.count)
    Assert.assertEquals(4, rcugasSospensioni.columns.length)
  }

  class RcugasSospensioniPDAOMock extends RcugasSospensioniPDAO {
    val sqlContext: SQLContext = Environment.sqlContext
    import sqlContext.implicits._

    val rcugasSospensioni: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("nIdPdr1", "fornitura1", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0"),
      ("nIdPdr1", "fornitura2", "2022-10-16 00:00:00.0", "2022-11-31 00:00:00.0"),

      ("nIdPdr2", "fornitura3", "2022-09-01 00:00:00.0", "2022-11-01 00:00:00.0"),

      ("nIdPdr3", "fornitura4", "2022-09-01 00:00:00.0", "2022-09-27 00:00:00.0"),

      ("nIdPdr4", "fornitura5", "2022-11-01 00:00:00.0", "2022-11-31 00:00:00.0"),

      ("nIdPdr5", "fornitura6", "2022-09-01 00:00:00.0", "2022-09-30 00:00:00.0"),
      ("nIdPdr5", "fornitura7", "2022-11-01 00:00:00.0", "2022-11-31 00:00:00.0")
    )).toDF(RcugasSospensioniPSchema.getValues: _*)

    override def readParquet: DataFrame = rcugasSospensioni
  }
}
