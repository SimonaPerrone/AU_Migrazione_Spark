package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.rcugas.VRcugasDistributorePModel
import it.eng.au.portaleConsumi.schema.rcugas.VRcugasDistributorePSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class VRcugasDistributorePDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark
  import spark.implicits._

  case class VRcugasDistributorePDaoMock(ds: Dataset[VRcugasDistributorePModel]) extends VRcugasDistributorePDao {
    override def read(columns: List[String]): Dataset[VRcugasDistributorePModel] = ds
  }

  def testReadPdrCategoria(): Unit = {
    val distr1 = VRcugasDistributorePModel(n_id_distributore = "1", t_rag_soc = null)
    val distr2 = VRcugasDistributorePModel(n_id_distributore = "2", t_rag_soc = "2", d_data_fine = Timestamp.valueOf("2023-01-01 00:00:00"))
    val distr3 = VRcugasDistributorePModel(n_id_distributore = "3", t_rag_soc = "3", d_data_fine = null)
    val ds = Seq(distr1, distr2, distr3).toDS()

    val expected = Map(
      "3" -> distr3
    )

    val result = VRcugasDistributorePDaoMock(ds).readAttivi().cache()

    Assert.assertEquals(1, result.count())
    Assert.assertEquals(expected("3"), result.where(col(VRcugasDistributorePSchema.n_id_distributore) === "3").collect().head)
  }

}
