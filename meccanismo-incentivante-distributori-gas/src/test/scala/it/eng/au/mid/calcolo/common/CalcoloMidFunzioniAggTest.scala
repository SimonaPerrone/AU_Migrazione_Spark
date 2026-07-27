package it.eng.au.mid.calcolo.common

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.calcolo.CalcoloMidFunzioniAgg
import it.eng.au.mid.model.flow.calcolo.{DailyConsumptionEsclusiModel, DailyConsumptionIncoerentiModel}
import org.apache.spark.sql.functions.col
import org.junit.Assert


class CalcoloMidFunzioniAggTest extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  def testLeggiPdrAnomaliAgg(): Unit = {
    val executionId = 1
    val annomese1 = "202302"
    val annomese2 = "202302"
    val annomeseLista = List(annomese1, annomese2)
    val incoerentiDs = Seq(
      DailyConsumptionIncoerentiModel(pdr = "1", annomese = annomese1, session = "SBG", ispdranomalousgdm = true, executionid = 1),
      DailyConsumptionIncoerentiModel(pdr = "1", annomese = annomese1, session = "SBG", ispdranomalousgdm = true, executionid = 2),
      DailyConsumptionIncoerentiModel(pdr = "2", annomese = annomese1, session = "SBG", ispdranomalousgdm = false, executionid = 2)
    ).toDS()
    val esclusiDs = Seq(
      DailyConsumptionEsclusiModel(pdr = "2", annomese2, session = "SBG", executionid = 1)
    ).toDS()

    val result = CalcoloMidFunzioniAgg.leggiPdrAnomaliAgg(incoerentiDs, esclusiDs, executionId, annomeseLista).cache()
    Assert.assertEquals(2, result.count())
    Assert.assertEquals(2, result.select("pdr").distinct().count())
    Assert.assertEquals(0, result.where(col("executionId") =!= executionId).count())
  }

}
