package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.schema.cig.PdrGSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class RecoveryControllerTest extends EnvironmentSparkTest {
  def testFilter(): Unit = {
    Environment.setProperty("recovery.csv.path", "src/test/resources/recovery_file.csv")
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrG = Environment.sparkContext.parallelize(Seq(
    ("pdr1", "udd1", "distr1"),
    ("pdr2", "udd1", "distr1"),
    ("pdr3", "udd1", "distr2"),
    ("pdr4", "udd1", "distr3"),
    ("pdr5", "udd2", "distr3"),
    ("pdr6", "udd3", "distr4"),
    ("pdr7", "udd4", "distr5")
    )).toDF(
      PdrGSchema.codice_pdr,
      PdrGSchema.piva_udd,
      PdrGSchema.piva_distr
    )

    val result = RecoveryController.filter(pdrG)
    result.show

    Assert.assertEquals(4, result.count)
    Assert.assertEquals(3, result.columns.length)
    Assert.assertEquals(2, result.where(col(PdrGSchema.piva_udd) === lit("udd1")).count)
    Assert.assertEquals(0, result.where(col(PdrGSchema.piva_udd) === lit("udd4")).count)
  }
}
