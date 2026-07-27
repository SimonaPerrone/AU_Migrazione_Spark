package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasPdrMisuratorePModel
import it.eng.au.portaleConsumi.schema.rcugas.RcugasPdrMisuratorePSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.{Date, Timestamp}

class RcugasPdrMisuratorePDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark
  import spark.implicits._

  case class RcugasPdrMisuratorePDaoMock(ds: Dataset[RcugasPdrMisuratorePModel]) extends RcugasPdrMisuratorePDao {
    override def read(columns: List[String]): Dataset[RcugasPdrMisuratorePModel] = ds
  }

  def testReadMisuratoriInstallati(): Unit = {
    val ds = Seq(
      RcugasPdrMisuratorePModel(
        n_id_pdr = "1", t_matricola_misuratore = "matricola1", t_classe_misuratore = "classe1", n_coeff_correzione = "0.1",
        t_data_inst_misuratore = Date.valueOf("2023-01-01")
      ),
      RcugasPdrMisuratorePModel
      (n_id_pdr = "1", t_matricola_misuratore = "matricola11", t_classe_misuratore = "classe11", n_coeff_correzione = "0.11",
        t_data_inst_misuratore = Date.valueOf("2023-01-11")
      ),
      RcugasPdrMisuratorePModel(n_id_pdr = "2", t_matricola_misuratore = "matricola22", t_classe_misuratore = "classe22",
        n_coeff_correzione = "0.22", t_data_inst_misuratore = Date.valueOf("2023-01-01")
      )
    ).toDS()

    val expected = Map(
      "1" -> Seq("1", "matricola1", "classe1", "0.1", Date.valueOf("2023-01-01")),
      "11" -> Seq("1", "matricola11", "classe11", "0.11", Date.valueOf("2023-01-11")),
      "22" -> Seq("2", "matricola22", "classe22", "0.22", Date.valueOf("2023-01-01"))
    )

    val result = RcugasPdrMisuratorePDaoMock(ds).readMisuratoriInstallati()

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(0, result.where(col(RcugasPdrMisuratorePSchema.t_matricola_misuratore) === "matricola2").count())
    Assert.assertEquals(expected("1"), result.where(col(RcugasPdrMisuratorePSchema.t_matricola_misuratore) === "matricola1").collect().head.toSeq)
    Assert.assertEquals(expected("11"), result.where(col(RcugasPdrMisuratorePSchema.t_matricola_misuratore) === "matricola11").collect().head.toSeq)
    Assert.assertEquals(expected("22"), result.where(col(RcugasPdrMisuratorePSchema.t_matricola_misuratore) === "matricola22").collect().head.toSeq)
  }

}
