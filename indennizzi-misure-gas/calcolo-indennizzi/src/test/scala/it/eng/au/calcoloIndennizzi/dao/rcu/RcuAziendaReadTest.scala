package it.eng.au.calcoloIndennizzi.dao.rcu

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.schema.rcu.RcuAziendaSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.Assert

class RcuAziendaReadTest extends EnvironmentSparkTest {
  def testReadRcuAzienda(): Unit = {
    val rcuAziendaDAOMock = new RcuAziendaDAOMock
    val rcuAzienda = rcuAziendaDAOMock.get
    rcuAzienda.show

    Assert.assertEquals(3, rcuAzienda.count)
    Assert.assertEquals(2, rcuAzienda.columns.length)
  }

  class RcuAziendaDAOMock extends RcuAziendaDAO {
    val sqlContext: SQLContext = Environment.sqlContext
    import sqlContext.implicits._

    val rcuAzienda: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("piva1", "azienda1"),
      ("piva2", "azienda2"),
      ("piva3", "azienda3")
    ))
      .toDF(RcuAziendaSchema.getValues: _*)

    override def readParquet: DataFrame = rcuAzienda
  }
}