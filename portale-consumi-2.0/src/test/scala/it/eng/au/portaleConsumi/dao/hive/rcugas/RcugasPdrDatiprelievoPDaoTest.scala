package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasPdrDatiprelievoPModel
import it.eng.au.portaleConsumi.schema.rcugas.RcugasPdrDatiprelievoPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

class RcugasPdrDatiprelievoPDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark
  import spark.implicits._

  case class RcugasPdrDatiprelievoPDaoMock(ds: Dataset[RcugasPdrDatiprelievoPModel]) extends RcugasPdrDatiprelievoPDao {
    override def read(columns: List[String]): Dataset[RcugasPdrDatiprelievoPModel] = ds
  }

  def testReadPdrCategoria(): Unit = {
    val ds = Seq(
      RcugasPdrDatiprelievoPModel(n_id_pdr = "1", t_anno = "2023", t_cod_cat_uso = "C2"),
      RcugasPdrDatiprelievoPModel(n_id_pdr = "1", t_anno = "2022", t_cod_cat_uso = "C1"),
      RcugasPdrDatiprelievoPModel(n_id_pdr = "2", t_anno = "2021", t_cod_cat_uso = "C1")
    ).toDS()

    val expected = Map(
      "1" -> Seq("1", "C2"),
      "2" -> Seq("2", "C1")
    )

    val result = RcugasPdrDatiprelievoPDaoMock(ds).readPdrCategoria().cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(expected("1"), result.where(col(RcugasPdrDatiprelievoPSchema.n_id_pdr) === 1).collect().head.toSeq)
    Assert.assertEquals(expected("2"), result.where(col(RcugasPdrDatiprelievoPSchema.n_id_pdr) === 2).collect().head.toSeq)
  }

}
