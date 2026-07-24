package it.eng.au.gse.common.controller

import it.eng.au.gse.common.EnvironmentSparkTest
import it.eng.au.gse.common.schema.dwh.DwhConsumiSchema
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.functions.col
import org.junit.Assert

class PrepareCommonTest extends EnvironmentSparkTest {
  def testPrepareDwhConsumi(): Unit = {
    Environment.setProperty("year", "2023")
    Environment.setProperty("month", "5")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dwhConsumi = Seq(
      ("pod1", 2023, 5, 1),
      ("pod1", 2023, 5, 2),
      ("pod2", 2023, 5, 1),
      ("pod2", 2023, 6, 2),
      ("pod3", 2023, 7, 1)
    ).toDF(DwhConsumiSchema.pod14, DwhConsumiSchema.anno, DwhConsumiSchema.mese, DwhConsumiSchema.versione)

    val yearMonthList = List((2023, 5), (2023, 6))

    val output = PrepareCommon.prepareDwhConsumi(dwhConsumi, yearMonthList)
    output.show

    Assert.assertEquals(4, output.count)
    Assert.assertEquals(2, output.where(col(DwhConsumiSchema.pod14) === "pod1").count)
    Assert.assertEquals(2, output.where(col(DwhConsumiSchema.pod14) === "pod2").count)
  }
}
