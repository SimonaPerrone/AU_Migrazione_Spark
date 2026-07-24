package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasConnessioniDistr2Schema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.Assert

class RcugasConnessioniDistr2ReadTest extends EnvironmentSparkTest {
  def testReadRcugasConnessioniDistr2(): Unit = {
    Environment.setProperty("days.in.month", "31")

    val rcugasConnessioniDistr2DAOMock = new RcugasConnessioniDistr2DAOMock
    val rcugasConnessioniDistr2 = rcugasConnessioniDistr2DAOMock.get
    rcugasConnessioniDistr2.show

    Assert.assertEquals(2, rcugasConnessioniDistr2.count)
    Assert.assertEquals(2, rcugasConnessioniDistr2.columns.length)
  }

  class RcugasConnessioniDistr2DAOMock extends RcugasConnessioniDistr2DAO {
    val sqlContext: SQLContext = Environment.sqlContext
    import sqlContext.implicits._

    val rcugasConnessioniDistr2: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("pdr1", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0", "distr1"),
      ("pdr1", "2022-10-16 00:00:00.0", "2022-11-31 00:00:00.0", "distr1"),

      ("pdr2", "2022-09-01 00:00:00.0", "2022-11-01 00:00:00.0", "distr2"),

      ("pdr3", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0", "distr3"),
      ("pdr3", "2022-10-20 00:00:00.0", "2022-10-31 00:00:00.0", "distr3"),

      ("pdr4", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0", "distr5"),
      ("pdr4", "2022-10-16 00:00:00.0", "2022-10-31 00:00:00.0", "distr6"),
      ("pdr4", "2022-10-01 00:00:00.0", "2022-10-31 00:00:00.0", "distr7")
    ))
      .toDF(RcugasConnessioniDistr2Schema.getValues: _*)

    override def readParquet: DataFrame = rcugasConnessioniDistr2
  }
}