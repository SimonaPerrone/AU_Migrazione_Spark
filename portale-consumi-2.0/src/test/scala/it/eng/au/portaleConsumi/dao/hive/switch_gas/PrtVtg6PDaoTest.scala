package it.eng.au.portaleConsumi.dao.hive.switch_gas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.switch_gas.PrtVtg6PModel
import it.eng.au.portaleConsumi.schema.flow.misure.MisureGasSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class PrtVtg6PDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark
  import spark.implicits._

  case class HiveDaoMock(ds: Dataset[PrtVtg6PModel]) extends PrtVtg6PDao {
    override def readTable(): DataFrame = ds.toDF()
  }

  def testReadNuoveMisure(): Unit = {
    val limiteAnnoMese = "202001"
    val ultimoCalcoloTs = Timestamp.valueOf("2023-01-01 00:00:00")
    val dataCalcoloTs = Timestamp.valueOf("2023-12-01 00:00:00")

    val ds = Seq(
      PrtVtg6PModel(
        t_codice_pdr = "pdr1",
        d_data_mis_eff_ts = Timestamp.valueOf("2023-01-05 00:00:00"),
        t_segn_mis_eff = 1,
        t_segn_mis_sost = 2,
        t_tipo_lettura = "E",
        d_caricamento = Timestamp.valueOf("2023-01-03 00:00:00")
      ),
      PrtVtg6PModel(
        t_codice_pdr = "pdr1",
        d_data_mis_eff_ts = Timestamp.valueOf("2023-01-05 00:00:00"),
        t_segn_mis_eff = 1,
        t_segn_mis_sost = 2,
        t_tipo_lettura = "S",
        d_caricamento = Timestamp.valueOf("2023-01-03 00:00:00")
      ),
      PrtVtg6PModel(
        t_codice_pdr = "pdr2",
        d_data_mis_eff_ts = Timestamp.valueOf("2023-01-05 00:00:00"),
        t_segn_mis_eff = null,
        t_segn_mis_sost = 2,
        t_tipo_lettura = "E",
        d_caricamento = Timestamp.valueOf("2023-01-03 00:00:00")
      )
    ).toDS()

    val expected1 = MisureGasModel(codice_pdr = "pdr1", lettura = 1, data_lettura = Timestamp.valueOf("2023-01-05 00:00:00"),
      data_caricamento = Timestamp.valueOf("2023-01-03 00:00:00"), annomese = "202301", flusso = "vtg")
    val expected2 = MisureGasModel(codice_pdr = "pdr2", lettura = 2, data_lettura = Timestamp.valueOf("2023-01-05 00:00:00"),
      data_caricamento = Timestamp.valueOf("2023-01-03 00:00:00"), annomese = "202301", flusso = "vtg")

    val result = HiveDaoMock(ds).readNuoveMisure(limiteAnnoMese, ultimoCalcoloTs, dataCalcoloTs).cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(expected1, result.where(col(MisureGasSchema.codice_pdr) === "pdr1").collect()(0))
    Assert.assertEquals(expected2, result.where(col(MisureGasSchema.codice_pdr) === "pdr2").collect()(0))
  }

}
