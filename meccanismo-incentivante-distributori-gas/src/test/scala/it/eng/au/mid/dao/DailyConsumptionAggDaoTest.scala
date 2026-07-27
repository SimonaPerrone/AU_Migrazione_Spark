package it.eng.au.mid.dao

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao.DailyConsumptionAggDaoMock
import it.eng.au.mid.model.flow
import it.eng.au.mid.model.flow.DailyConsumptionModel
import org.junit.Assert

import scala.collection.mutable


class DailyConsumptionAggDaoTest extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  def testInfoUltimaEsecuzione_fin(): Unit = {
    val session = "AGG_S2_FIN"
    val executionId = 123
    val ds = Seq(
      flow.DailyConsumptionModel(annomese = "202301", session = session, executionid = executionId),
      flow.DailyConsumptionModel(annomese = "202302", session = session, executionid = executionId),
      flow.DailyConsumptionModel(annomese = "202304", session = session, executionid = 333) // no, execId errato
    ).toDS()

    val result = new DailyConsumptionAggDaoMock(ds).infoUltimaEsecuzione(executionId)

    Assert.assertEquals(List("202301", "202302").sorted, result("annomeseAnomali").asInstanceOf[mutable.WrappedArray[String]].toList.sorted)
    Assert.assertEquals("AGG", result("sessione").asInstanceOf[String])
    Assert.assertEquals(session, result("tracciatura").asInstanceOf[String])
    Assert.assertEquals(executionId, result("executionId").asInstanceOf[Long])
  }

  def testInfoUltimaEsecuzione_pre(): Unit = {
    val session = "AGG_S2_PRE"
    val executionId = 123
    val ds = Seq(
      flow.DailyConsumptionModel(annomese = "202301", session = session, executionid = executionId)
    ).toDS()

    val result = new DailyConsumptionAggDaoMock(ds).infoUltimaEsecuzione(executionId)

    Assert.assertEquals(List("202301").sorted, result("annomeseAnomali").asInstanceOf[mutable.WrappedArray[String]].toList.sorted)
    Assert.assertEquals("AGG", result("sessione").asInstanceOf[String])
    Assert.assertEquals(session, result("tracciatura").asInstanceOf[String])
    Assert.assertEquals(executionId, result("executionId").asInstanceOf[Long])
  }

  def testInfoUltimaEsecuzione_vuoto(): Unit = {
    val executionId = 123
    val ds = List.empty[DailyConsumptionModel].toDS()
    try {
      new DailyConsumptionAggDaoMock(ds).infoUltimaEsecuzione(executionId)
      Assert.assertTrue(false)
    }
    catch {
      case _: Exception => Assert.assertTrue(true)
    }
  }
}
