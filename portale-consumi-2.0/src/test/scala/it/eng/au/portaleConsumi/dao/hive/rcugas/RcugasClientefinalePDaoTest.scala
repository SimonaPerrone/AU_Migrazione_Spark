package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasClientefinalePModel
import it.eng.au.portaleConsumi.schema.rcugas.RcugasClientefinalePSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

class RcugasClientefinalePDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark

  import spark.implicits._

  case class RcugasClientefinalePDaoMock(ds: Dataset[RcugasClientefinalePModel]) extends RcugasClientefinalePDao {
    override def read(columns: List[String]): Dataset[RcugasClientefinalePModel] = ds
  }

  def testReadValidati(): Unit = {
    val cliente1 = RcugasClientefinalePModel(n_id_cliente = "1", t_codice_fiscale = "1", t_partita_iva = "1")
    val cliente2 = RcugasClientefinalePModel(n_id_cliente = "2", t_codice_fiscale = "2", t_partita_iva = null)
    val cliente3 = RcugasClientefinalePModel(n_id_cliente = "3", t_codice_fiscale = null, t_partita_iva = "3")
    val cliente4 = RcugasClientefinalePModel(n_id_cliente = "4", t_codice_fiscale = null, t_partita_iva = null)
    val ds = Seq(cliente1, cliente2, cliente3, cliente4).toDS()

    val expected = Map(
      "1" -> cliente1,
      "2" -> cliente2,
      "3" -> RcugasClientefinalePModel(n_id_cliente = "3", t_codice_fiscale = "3", t_partita_iva = "3")
    )

    val result = RcugasClientefinalePDaoMock(ds).readValidati().cache()

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(expected("1"), result.where(col(RcugasClientefinalePSchema.n_id_cliente) === "1").collect().head)
    Assert.assertEquals(expected("2"), result.where(col(RcugasClientefinalePSchema.n_id_cliente) === "2").collect().head)
    Assert.assertEquals(expected("3"), result.where(col(RcugasClientefinalePSchema.n_id_cliente) === "3").collect().head)
    Assert.assertEquals(0, result.where(col(RcugasClientefinalePSchema.n_id_cliente) === "4").count())
  }

}
