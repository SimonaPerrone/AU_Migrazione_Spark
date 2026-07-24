package it.eng.au.pubblicazione_cce.dao

import it.eng.au.pubblicazione_cce.EnvironmentSparkTest
import it.eng.au.pubblicazione_cce.dao.cce.CceCalcoloDao
import it.eng.au.pubblicazione_cce.mock.dao.CceCalcoloDaoMock
import it.eng.au.pubblicazione_cce.model.cce.CceCalcoloMisureModel
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.Assert

class CceCalcoloPDaoTest() extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  def testRead_pad(): Unit = {
    val ds = Seq(
      CceCalcoloMisureModel(pod = null, data_misura = null, giorno = null, data_calcolo = null, nome_file = null,
        anno = "2024", mese = "1",
        executionid = null)
    ).toDS
    val result = new CceCalcoloDaoMock(ds).read()

    Assert.assertEquals("01", result.head.mese)
  }

  def testRead(): Unit = {
    val ds = Seq(
      CceCalcoloMisureModel(pod = null, data_misura = null, giorno = null, data_calcolo = null, nome_file = null,
        anno = "2024", mese = "12",
        executionid = null)
    ).toDS
    val result = new CceCalcoloDaoMock(ds).read()

    Assert.assertEquals("12", result.head.mese)
  }

}
