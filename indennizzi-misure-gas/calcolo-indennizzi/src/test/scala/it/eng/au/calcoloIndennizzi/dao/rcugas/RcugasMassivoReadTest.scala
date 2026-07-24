package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasMassivoPSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.Assert

class RcugasMassivoReadTest extends EnvironmentSparkTest {
  def testReadRcugasMassivo(): Unit = {
    Environment.setProperty("year.month", "202210")
    Environment.setProperty("days.in.month", "31")

    val rcugasMassivoPDAOMock = new RcugasMassivoPDAOMock
    val rcugasMassivo = rcugasMassivoPDAOMock.get
    rcugasMassivo.show

    Assert.assertEquals(4, rcugasMassivo.count)
    Assert.assertEquals(7, rcugasMassivo.columns.length)
  }

  class RcugasMassivoPDAOMock extends RcugasMassivoPDAO {
    val sqlContext: SQLContext = Environment.sqlContext
    import sqlContext.implicits._

    val rcugasMassivo: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("nIdPdr1", "pdr1", "fornitura1", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0", "udd1"),
      ("nIdPdr1", "pdr1", "fornitura2", "2022-10-16 00:00:00.0", "2022-11-31 00:00:00.0", "udd1"),

      ("nIdPdr2", "pdr2", "fornitura3", "2022-09-01 00:00:00.0", "2022-10-29 00:00:00.0", "udd2"),
      ("nIdPdr2", "pdr2", "fornitura4", "2022-10-30 00:00:00.0", "2022-11-01 00:00:00.0", "udd2"),

      ("nIdPdr3", "pdr3", "fornitura5", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0", "udd3"),
      ("nIdPdr3", "pdr3", "fornitura6", "2022-10-20 00:00:00.0", "2022-10-31 00:00:00.0", "udd3"),

      ("nIdPdr4", "pdr4", "fornitura7", "2022-10-01 00:00:00.0", "2022-10-15 00:00:00.0", "udd5"),
      ("nIdPdr4", "pdr4", "fornitura8", "2022-10-16 00:00:00.0", "2022-10-31 00:00:00.0", "udd6"),
      ("nIdPdr4", "pdr4", "fornitura9", "2022-10-01 00:00:00.0", "2022-10-31 00:00:00.0", "udd7")
    ))
      .toDF(RcugasMassivoPSchema.getValues: _*)

    override def readParquet: DataFrame = rcugasMassivo
  }
}