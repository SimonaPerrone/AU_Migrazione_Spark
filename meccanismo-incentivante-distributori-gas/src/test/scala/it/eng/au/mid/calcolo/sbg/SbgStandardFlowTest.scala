package it.eng.au.mid.calcolo.sbg

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.calcolo.SbgStandardFlow
import it.eng.au.mid.model.flow.calcolo.{DailyConsumptionEsclusiModel, DailyConsumptionIncoerentiModel}
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

class SbgStandardFlowTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  def testLeggiPdrAnomaliSbg(): Unit = {
    val executionId = 1
    val annomese = "202302"
    val incoerentiDs = Seq(
      DailyConsumptionIncoerentiModel(pdr = "1", annomese = annomese, session = "SBG", ispdranomalousgdm = true, executionid = 1),
      DailyConsumptionIncoerentiModel(pdr = "1", annomese = annomese, session = "SBG", ispdranomalousgdm = true, executionid = 2),
      DailyConsumptionIncoerentiModel(pdr = "2", annomese = annomese, session = "SBG", ispdranomalousgdm = false, executionid = 2)
    ).toDS()
    val esclusiDs = Seq(
      DailyConsumptionEsclusiModel(pdr = "2", annomese, session = "SBG", executionid = 1)
    ).toDS()

    val result = new SbgStandardFlow().leggiPdrAnomaliSbg(incoerentiDs, esclusiDs, executionId, annomese).cache()
    Assert.assertEquals(2, result.count())
    Assert.assertEquals(2, result.select("pdr").distinct().count())
    Assert.assertEquals(0, result.where(col("executionId") =!= executionId).count())
  }

  def testLeggiAnomaliMidPrecedenti(): Unit = {
    val annomese = "202301"
    val mid = Seq(
      MidContatoriModel(pdr = "1", executionid_tracciatura = 1, stato = CostantiMid.STATO_VALIDO, annomese = annomese),
      MidContatoriModel(pdr = "1", executionid_tracciatura = 2, stato = CostantiMid.STATO_FORZATO, annomese = annomese), // solo questo deve ritornare
      MidContatoriModel(pdr = "2", executionid_tracciatura = 2, stato = CostantiMid.STATO_INVALIDO, annomese = annomese),
      MidContatoriModel(pdr = "2", executionid_tracciatura = 2, stato = CostantiMid.STATO_FORZATO, annomese = "202212")
    ).toDS()
    val result = new SbgStandardFlow().leggiAnomaliMidPrecedenti(mid, annomese).cache()
    Assert.assertEquals(1L, result.count())
    Assert.assertEquals(2L, result.head().executionid_tracciatura)
  }

}
