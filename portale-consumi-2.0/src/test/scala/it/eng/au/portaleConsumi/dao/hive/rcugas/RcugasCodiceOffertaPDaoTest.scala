package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasCodiceOffertaPModel
import it.eng.au.portaleConsumi.schema.rcugas.RcugasCodiceOffertaPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

class RcugasCodiceOffertaPDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark

  import spark.implicits._

  case class RcugasCodiceOffertaPDaoMock(ds: Dataset[RcugasCodiceOffertaPModel]) extends RcugasCodiceOffertaPDao {
    override def read(columns: List[String]): Dataset[RcugasCodiceOffertaPModel] = ds
  }

  def testReadAttivi(): Unit = {
    val ds = Seq(
      RcugasCodiceOffertaPModel(n_id_fornitura = "forn1", t_codice_offerta = "off1", d_data_fine = null),
      RcugasCodiceOffertaPModel(n_id_fornitura = "forn2", t_codice_offerta = "off2", d_data_fine = "2023-12-31")
    ).toDS()

    val result = RcugasCodiceOffertaPDaoMock(ds).readAttivi().cache()

    Assert.assertEquals(1, result.count())
    Assert.assertEquals(1, result.where(col(RcugasCodiceOffertaPSchema.n_id_fornitura) === "forn1").count())
  }

}
