package it.eng.au.mid.calcolo.agg.standard

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.calcolo.{AggStandardFlow, CalcoloMidFunzioniAgg}
import it.eng.au.mid.model.flow.calcolo.{DailyConsumptionEsclusiModel, DailyConsumptionIncoerentiModel}
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import it.eng.au.mid.schema.flow.calcolo.DailyConsumptionIncoerentiSchema
import it.eng.au.mid.schema.hive.mid.MidContatoriSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

class AggStandardFlowTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  def testLeggiPdrAnomaliAgg(): Unit = {
    val executionId = 1
    val annomeseLista = List("202302", "202301")
    val incoerentiDs = Seq(
      DailyConsumptionIncoerentiModel(pdr = "1", annomese = "202302", session = "AGG", ispdranomalousgdm = true, executionid = 1),
      DailyConsumptionIncoerentiModel(pdr = "1", annomese = "202301", session = "AGG", ispdranomalousgdm = true, executionid = 2),
      DailyConsumptionIncoerentiModel(pdr = "2", annomese = "202301", session = "AGG", ispdranomalousgdm = false, executionid = 2)
    ).toDS()
    val esclusiDs = Seq(
      DailyConsumptionEsclusiModel(pdr = "2", "202302", session = "AGG", executionid = 1)
    ).toDS()

    val result = CalcoloMidFunzioniAgg.leggiPdrAnomaliAgg(incoerentiDs, esclusiDs, executionId, annomeseLista).cache()
    Assert.assertEquals(2, result.count())
    Assert.assertEquals(2, result.select("pdr").distinct().count())
    Assert.assertEquals(0, result.where(col(DailyConsumptionIncoerentiSchema.executionid) =!= executionId).count())
  }


  def testLeggiAnomaliMidPrecedenti(): Unit = {
    val annomese1 = "202301"
    val annomese2 = "202302"
    val execId = 2L
    val mid = Seq(
      MidContatoriModel(pdr = "1", executionid_tracciatura = 1, annomese = annomese1),
      MidContatoriModel(pdr = "1", executionid_tracciatura = execId, stato = CostantiMid.STATO_VALIDO, annomese = annomese1), // deve ritornare
      MidContatoriModel(pdr = "2", executionid_tracciatura = execId, stato = CostantiMid.STATO_VALIDO, annomese = annomese1), // deve ritornare
      MidContatoriModel(pdr = "2", executionid_tracciatura = execId, stato = CostantiMid.STATO_VALIDO, annomese = annomese2), // deve ritornare
      MidContatoriModel(pdr = "3", executionid_tracciatura = execId, stato = CostantiMid.STATO_VALIDO, annomese = "202212")
    ).toDS()
    val result = new AggStandardFlow().leggiAnomaliMidPrecedenti(mid, List(annomese1, annomese2)).cache()

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(1, result.where(col(MidContatoriSchema.pdr) === "1" and col(MidContatoriSchema.annomese) === annomese1).count())
    Assert.assertEquals(1, result.where(col(MidContatoriSchema.pdr) === "2" and col(MidContatoriSchema.annomese) === annomese1).count())
    Assert.assertEquals(1, result.where(col(MidContatoriSchema.pdr) === "2" and col(MidContatoriSchema.annomese) === annomese2).count())
    Assert.assertEquals(execId, result.head().executionid_tracciatura)
  }


}
