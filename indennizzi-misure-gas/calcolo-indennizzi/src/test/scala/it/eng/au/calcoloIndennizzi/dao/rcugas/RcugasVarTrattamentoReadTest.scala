package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasVarTrattamentoPSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.Assert

class RcugasVarTrattamentoReadTest extends EnvironmentSparkTest {
  def testReadRcugasVarTrattamento(): Unit = {
    Environment.setProperty("days.in.month", "31")

    val rcugasVarTrattamentoPDAOMock = new RcugasVarTrattamentoPDAOMock
    val rcugasVarTrattamento = rcugasVarTrattamentoPDAOMock.get
    rcugasVarTrattamento.show

    Assert.assertEquals(2, rcugasVarTrattamento.count)
    Assert.assertEquals(1, rcugasVarTrattamento.columns.length)

  }

  class RcugasVarTrattamentoPDAOMock extends RcugasVarTrattamentoPDAO {
    val sqlContext: SQLContext = Environment.sqlContext
    import sqlContext.implicits._

    val rcugasVarTrattamento: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("nIdPdr1", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0", "G"),
      ("nIdPdr1", "2022-10-16 00:00:00.0", "2022-11-31 00:00:00.0", "G"),

      ("nIdPdr2", "2022-09-01 00:00:00.0", "2022-11-01 00:00:00.0", "G"),

      ("nIdPdr3", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0", "G"),
      ("nIdPdr3", "2022-10-20 00:00:00.0", "2022-10-31 00:00:00.0", "G"),

      ("nIdPdr4", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0", "G"),
      ("nIdPdr4", "2022-10-16 00:00:00.0", "2022-10-31 00:00:00.0", "Y"),
      ("nIdPdr4", "2022-11-01 00:00:00.0", "2022-11-31 00:00:00.0", "G")
    ))
      .toDF(RcugasVarTrattamentoPSchema.getValues: _*)

    override def readParquet: DataFrame = rcugasVarTrattamento
  }
}