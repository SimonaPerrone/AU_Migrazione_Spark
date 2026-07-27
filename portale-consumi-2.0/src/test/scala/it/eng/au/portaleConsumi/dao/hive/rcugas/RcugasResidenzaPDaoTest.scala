package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasResidenzaPModel
import it.eng.au.portaleConsumi.schema.rcugas.RcugasResidenzaPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class RcugasResidenzaPDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark
  import spark.implicits._

  case class RcugasResidenzaPDaoMock(ds: Dataset[RcugasResidenzaPModel]) extends RcugasResidenzaPDao {
    override def read(columns: List[String]): Dataset[RcugasResidenzaPModel] = ds
  }

  def testReadResidenzeForniture(): Unit = {
    val ds = Seq(
      RcugasResidenzaPModel(n_id_fornitura = "1", t_residenza = "SI", d_aggiornamento = Timestamp.valueOf("2023-01-01 00:00:00")),
      RcugasResidenzaPModel(n_id_fornitura = "1", t_residenza = "NO", d_aggiornamento = Timestamp.valueOf("2023-01-02 00:00:00")),
      RcugasResidenzaPModel(n_id_fornitura = "2", t_residenza = "SI", d_aggiornamento = Timestamp.valueOf("2023-01-01 00:00:00"))
    ).toDS()

    val expected = Map(
      "1" -> Seq("1", "NO"),
      "2" -> Seq("2", "SI")
    )

    val result = RcugasResidenzaPDaoMock(ds).readResidenzeForniture().cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(expected("1"), result.where(col(RcugasResidenzaPSchema.n_id_fornitura) === 1).collect().head.toSeq)
    Assert.assertEquals(expected("2"), result.where(col(RcugasResidenzaPSchema.n_id_fornitura) === 2).collect().head.toSeq)
  }

}
