package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasFornituraPModel
import it.eng.au.portaleConsumi.schema.rcugas.RcugasFornituraPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class RcugasFornituraPDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark
  import spark.implicits._

  case class RcugasFornituraPDaoMock(ds: Dataset[RcugasFornituraPModel]) extends RcugasFornituraPDao {
    override def read(columns: List[String]): Dataset[RcugasFornituraPModel] = ds
  }

  def testReadProssimiSwitchPDR(): Unit = {
    val ds = Seq(
      RcugasFornituraPModel(n_id_fornitura = "1", d_data_fine = null),
      RcugasFornituraPModel(n_id_fornitura = "2", d_data_fine = Timestamp.valueOf("2023-01-01 00:00:00")),
      RcugasFornituraPModel(n_id_fornitura = "3", d_data_fine = Timestamp.valueOf("2022-01-01 00:00:00"))
    ).toDS()

    val result = RcugasFornituraPDaoMock(ds).read(Timestamp.valueOf("2023-01-01 00:00:00")).cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(1, result.where(col(RcugasFornituraPSchema.n_id_fornitura) === 1).count())
    Assert.assertEquals(1, result.where(col(RcugasFornituraPSchema.n_id_fornitura) === 2).count())
    Assert.assertEquals(0, result.where(col(RcugasFornituraPSchema.n_id_fornitura) === 3).count())
  }

}
