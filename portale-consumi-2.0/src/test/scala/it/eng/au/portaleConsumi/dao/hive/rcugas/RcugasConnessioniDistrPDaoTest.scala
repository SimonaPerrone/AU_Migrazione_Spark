package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasConnessioniDistrPModel
import it.eng.au.portaleConsumi.schema.rcugas.RcugasConnessioniDistrPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class RcugasConnessioniDistrPDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark
  import spark.implicits._

  case class RcugasConnessioniDistrPDaoMock(ds: Dataset[RcugasConnessioniDistrPModel]) extends RcugasConnessioniDistrPDao {
    override def read(columns: List[String]): Dataset[RcugasConnessioniDistrPModel] = ds
  }

  def testReadPdrCategoria(): Unit = {
    val pdr11 = RcugasConnessioniDistrPModel(n_id_pdr = "1", d_data_inizio_conn = Timestamp.valueOf("2023-01-01 00:00:00"), t_remi = "1")
    val pdr12 = RcugasConnessioniDistrPModel(n_id_pdr = "1", d_data_inizio_conn = Timestamp.valueOf("2023-01-02 00:00:00"), t_remi = "2")
    val pdr2 = RcugasConnessioniDistrPModel(n_id_pdr = "2", d_data_inizio_conn = Timestamp.valueOf("2023-01-02 00:00:00"), t_remi = "3")
    val ds = Seq(pdr11, pdr12, pdr2).toDS()

    val expected = Map(
      "1" -> pdr12,
      "2" -> pdr2
    )

    val result = RcugasConnessioniDistrPDaoMock(ds).readUltimoAggiornamento().cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(expected("1"), result.where(col(RcugasConnessioniDistrPSchema.n_id_pdr) === "1").collect().head)
    Assert.assertEquals(expected("2"), result.where(col(RcugasConnessioniDistrPSchema.n_id_pdr) === "2").collect().head)
  }

}
