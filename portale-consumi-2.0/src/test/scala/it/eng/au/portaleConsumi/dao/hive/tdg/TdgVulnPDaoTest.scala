package it.eng.au.portaleConsumi.dao.hive.tdg

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.tdg.TdgVulnPModel
import it.eng.au.portaleConsumi.schema.tdg.TdgVulnPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

class TdgVulnPDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark

  import spark.implicits._

  case class TdgVulnPDaoMock(ds: Dataset[TdgVulnPModel]) extends TdgVulnPDao {
    override def read(columns: List[String]): Dataset[TdgVulnPModel] = ds
  }

  def testReadAttivi(): Unit = {
    val ds = Seq(
      TdgVulnPModel(n_id_tdg_vuln = "1", n_id_pdr = "pdr1", n_id_cliente = "cli1", d_data_fine = null),
      TdgVulnPModel(n_id_tdg_vuln = "2", n_id_pdr = "pdr2", n_id_cliente = "cli2", d_data_fine = "2023-12-31")
    ).toDS()

    val result = TdgVulnPDaoMock(ds).readAttivi().cache()

    Assert.assertEquals(1, result.count())
    Assert.assertEquals(1, result.where(col(TdgVulnPSchema.n_id_tdg_vuln) === "1").count())
  }

}
