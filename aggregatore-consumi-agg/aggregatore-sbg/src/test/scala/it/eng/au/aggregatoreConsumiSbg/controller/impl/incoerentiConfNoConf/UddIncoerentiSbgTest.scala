package it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerentiConfNoConf

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.aggregatoreConsumiSbg.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerenti.UddIncoerentiSbg
import it.eng.au.aggregatoreConsumiSbg.controller.impl.incoerenti.confNoConf.UddIncoerentiConfNoConf
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

class UddIncoerentiSbgTest extends EnvironmentSparkTest {

  def testCsvConf(): Unit = {
    Environment.setProperty("pdr.anomali.listaC.enabled", "true")
    Environment.setProperty("pdr.anomali.listaD.enabled", "true")

    val df = UddIncoerentiConfNoConf.readCsvConfNoConf("src/test/resources/anomali/pdr_anomali_lista_C.csv").cache()

    df.show()

    Assert.assertEquals(df.filter(col("pdr") === "300PDR").rdd.map(_.getAs[String]("pdr")).collect.head, "300PDR")
  }

}
