package it.eng.au.portaleConsumi.dao.hive.switch_gas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.hive.switch_gas.PrtSwgPModel
import it.eng.au.portaleConsumi.schema.switch_gas.PrtSwgPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class PrtSwgPDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark
  import spark.implicits._

  case class PrtSwgPDaoMock(ds: Dataset[PrtSwgPModel]) extends PrtSwgPDao {
    override def read(columns: List[String]): Dataset[PrtSwgPModel] = ds
  }

  def testReadProssimiSwitchPDR(): Unit = {
    val statoNonValido = "B"
    val ds = Seq(
      PrtSwgPModel(t_codice_pdr = "1", t_stato = "", d_data_decorrenza = Timestamp.valueOf("2023-01-01 00:00:00")),
      PrtSwgPModel(t_codice_pdr = "2", t_stato = statoNonValido, d_data_decorrenza = Timestamp.valueOf("2023-01-01 00:00:00")),
      PrtSwgPModel(t_codice_pdr = "3", t_stato = "", d_data_decorrenza = Timestamp.valueOf("2022-01-01 00:00:00")),
      PrtSwgPModel(t_codice_pdr = "4", t_stato = "", d_data_decorrenza = Timestamp.valueOf("2023-01-03 00:00:00")),
      PrtSwgPModel(t_codice_pdr = "4", t_stato = statoNonValido, d_data_decorrenza = Timestamp.valueOf("2023-01-02 00:00:00")),
      PrtSwgPModel(t_codice_pdr = "5", t_stato = "", d_data_decorrenza = Timestamp.valueOf("2022-12-31 23:59:59")),
      PrtSwgPModel(t_codice_pdr = "5", t_stato = "", d_data_decorrenza = Timestamp.valueOf("2023-01-03 00:00:00")),
      PrtSwgPModel(t_codice_pdr = "5", t_stato = "", d_data_decorrenza = Timestamp.valueOf("2023-01-01 00:00:00"))
    ).toDS()

    val expected = Map(
      "1" -> Seq("1", Timestamp.valueOf("2023-01-01 00:00:00")),
      "4" -> Seq("4", Timestamp.valueOf("2023-01-03 00:00:00")),
      "5" -> Seq("5", Timestamp.valueOf("2023-01-01 00:00:00"))
    )

    val result = PrtSwgPDaoMock(ds).readProssimiSwitchPDR(Timestamp.valueOf("2023-01-01 00:00:00")).cache()

    Assert.assertEquals(expected("1"), result.where(col(PrtSwgPSchema.t_codice_pdr) === 1).collect().head.toSeq)
    Assert.assertEquals(0, result.where(col(PrtSwgPSchema.t_codice_pdr) === 2).count())
    Assert.assertEquals(0, result.where(col(PrtSwgPSchema.t_codice_pdr) === 3).count())
    Assert.assertEquals(expected("4"), result.where(col(PrtSwgPSchema.t_codice_pdr) === 4).collect().head.toSeq)
    Assert.assertEquals(expected("5"), result.where(col(PrtSwgPSchema.t_codice_pdr) === 5).collect().head.toSeq)
  }

}
