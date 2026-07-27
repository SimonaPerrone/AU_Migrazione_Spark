package it.eng.au.portaleConsumi.dao.hive.cmg_gas

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.cmg_gas.PrtCmgRmlPModel
import it.eng.au.portaleConsumi.schema.flow.misure.MisureGasSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class PrtCmgRmlPDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark

  import spark.implicits._

  case class HiveDaoMock(ds: Dataset[PrtCmgRmlPModel]) extends PrtCmgRmlPDao {
    override def readTable(): DataFrame = ds.toDF()
  }

  def testReadNuoveMisure(): Unit = {
    val limiteAnnoMese = "202001"
    val ultimoCalcoloTs = Timestamp.valueOf("2023-01-01 00:00:00")
    val dataCalcoloTs = Timestamp.valueOf("2023-12-01 00:00:00")

    val ds = Seq(
      PrtCmgRmlPModel(
        cod_pdr = "pdr1",
        annomese = "202001",
        annomese_riferimento = "202001",
        d_caricamento = Timestamp.valueOf("2023-01-02 00:00:00"),
        let_tot_prel = 0,
        data_racc = Timestamp.valueOf("2023-01-04 00:00:00")
      ),
      PrtCmgRmlPModel(
        cod_pdr = "pdr1",
        annomese = "201912",
        annomese_riferimento = "201912",
        d_caricamento = Timestamp.valueOf("2023-01-04 00:00:00"),
        let_tot_prel = 3,
        data_racc = Timestamp.valueOf("2023-01-05 00:00:00")
      )
    ).toDS()

    val expected = MisureGasModel(codice_pdr = "pdr1", lettura = 0, data_lettura = Timestamp.valueOf("2023-01-04 00:00:00"),
      data_caricamento = Timestamp.valueOf("2023-01-02 00:00:00"), annomese = "202001", flusso = "rml")

    val result = HiveDaoMock(ds).readNuoveMisure(limiteAnnoMese, ultimoCalcoloTs, dataCalcoloTs).cache()

    Assert.assertEquals(1, result.count())
    Assert.assertEquals(expected, result.where(col(MisureGasSchema.codice_pdr) === "pdr1").collect()(0))
  }

}
