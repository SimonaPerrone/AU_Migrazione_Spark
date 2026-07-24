package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasVarMisuratoreSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.Assert

class RcugasVarMisuratoreReadTest extends EnvironmentSparkTest {
  def testReadRcugasVarMisuratore(): Unit = {
    Environment.setProperty("days.in.month", "31")

    val rcugasVarMisuratorePDAOMock = new RcugasVarMisuratorePDAOMock
    val rcugasVarMisuratore = rcugasVarMisuratorePDAOMock.get
    rcugasVarMisuratore.show

    Assert.assertEquals(2, rcugasVarMisuratore.count)
    Assert.assertEquals(2, rcugasVarMisuratore.columns.length)

  }

  class RcugasVarMisuratorePDAOMock extends RcugasVarMisuratoreDAO {
    val sqlContext: SQLContext = Environment.sqlContext
    import sqlContext.implicits._

    val rcugasVarMisuratore: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("nIdPdr1", "G10", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0"),
      ("nIdPdr1", "G4", "2022-10-16 00:00:00.0", "2022-11-31 00:00:00.0"),

      ("nIdPdr2", "G10", "2022-09-01 00:00:00.0", "2022-11-01 00:00:00.0"),

      ("nIdPdr3", "G16", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0"),
      ("nIdPdr3", "G25", "2022-10-20 00:00:00.0", "2022-10-31 00:00:00.0"),

      ("nIdPdr4", "G100", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0"),
      ("nIdPdr4", "G10", "2022-10-16 00:00:00.0", "2022-10-31 00:00:00.0"),
      ("nIdPdr4", "G4", "2022-11-01 00:00:00.0", "2022-11-31 00:00:00.0")
    ))
      .toDF(RcugasVarMisuratoreSchema.getValues: _*)

    override def readParquet: DataFrame = rcugasVarMisuratore
  }
}
