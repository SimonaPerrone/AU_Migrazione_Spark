package it.eng.au.mid.dao

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao.DailyConsumptionSbgDaoMock
import it.eng.au.mid.model.flow.DailyConsumptionModel
import org.junit.Assert

import scala.collection.immutable.HashMap

class DailyConsumptionSbgDaoTest extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  def testInfoUltimaEsecuzione(): Unit = {
    val annomese = "202401"
    val session = "SBG"
    val executionId = 123L

    val sbgDS = Seq(
      // risultato atteso
      DailyConsumptionModel(annomese = annomese, session = session, executionid = executionId),
      // Da scartare: executionId minore
      DailyConsumptionModel(annomese = annomese, session = session, executionid = 111L),
      // Da scartare: annomese differente
      DailyConsumptionModel(annomese = "202402", session = session, executionid = executionId),
      // Da scartare: stesso annomese, sessione non corretta e con executionId maggiore
      DailyConsumptionModel(annomese = annomese, session = "CCG", executionid = 999L)
    ).toDS

    val expected = HashMap(
      "annomese" -> annomese,
      "sessione" -> session,
      "tracciatura" -> session,
      "executionId" -> executionId
    )
    val result = new DailyConsumptionSbgDaoMock(sbgDS).infoUltimaEsecuzione(annomese)
    Assert.assertEquals(expected, result)
  }

}
