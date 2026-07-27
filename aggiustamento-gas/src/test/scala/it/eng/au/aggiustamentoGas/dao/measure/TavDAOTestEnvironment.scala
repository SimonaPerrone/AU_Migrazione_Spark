package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.measure.Tav
import it.eng.au.aggiustamentoGas.schema.measure.TavSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class TavDAOTestEnvironment extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = TAVDAOMock.get("202101", "202101", getTreatment = false)
    Assert.assertEquals(4, result.count)
    Assert.assertTrue(result.filter(_.pdr == "1").first().date.isEmpty)
    Assert.assertTrue(result.filter(_.pdr == "1").first().asInstanceOf[Tav].outcome.isDefined)
    Assert.assertTrue(result.filter(_.pdr == "1").first().measure.isEmpty)
    Assert.assertTrue(result.filter(_.pdr == "1").first().converted.isEmpty)
    Assert.assertEquals("2020-05-14 05:46:22.99", result.filter(_.pdr == "1").first().dataCaricamento.get.toString("yyyy-MM-dd HH:mm:ss.SS"))

    Assert.assertEquals('v', result.filter(_.pdr == "2").first().asInstanceOf[Tav].outcome.get)
    Assert.assertEquals('v', result.filter(_.pdr == "3").first().asInstanceOf[Tav].outcome.get)
    Assert.assertEquals('v', result.filter(_.pdr == "4").first().asInstanceOf[Tav].outcome.get)
    Assert.assertEquals(0, result.filter(_.pdr == "5").count)

    Assert.assertEquals("02/02/2022", result.filter(_.pdr == "2").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("01/01/2021", result.filter(_.pdr == "3").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("04/04/2024", result.filter(_.pdr == "4").first().date.get.toString("dd/MM/yyyy"))

  }

  object TAVDAOMock extends TavDAO {
    override def readParquet: DataFrame = {
      val sqlCtx = Environment.getSpark.sqlContext
      import sqlCtx.implicits._
      // pdr==1 is an unmodified sample from parquet. Note that it has almost all fields to null. 
      // All the others are filled with manually forged data to complete test
      List(
        ("TAV", "1", "null", "null", "null", "null", "0005559244", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03679070288/2020/0513/00489490011_03679070288_202005_TMV0350_20200513203503_1.xml", Option(""), "",
          "", "2020-05-14T05:46:22.996999", "v  ", "202101"),
        ("TAV", "2", "02/02/2022", "null", "null", "null", "0000486011", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01178580997/2020/0423/00489490011_01178580997_202004_TMV0350_20200423203252_1.xml", Option(""), "",
          "", "2020-04-24T04:00:51.369432", "v", "202101"),
        ("TAV", "3", "2021/03/03", "01/01/2021", "null", "null", "0055329616", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12300020158/2020/0505/00489490011_12300020158_202004_TMV0350_20200505203351_34.xml", Option(""), "",
          "", "2020-05-06T05:17:03.890467", "v", "202101"),
        ("TAV", "4", "01/01/2021", "04/04/2024", "null", "null", "0005851658", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_06403060483/2020/0824/00489490011_06403060483_202008_TMV0350_20200824203108_1.xml", Option(""), "",
          "", "2020-08-25T08:51:22.887217", "v", "202101"),
        ("TAV", "5", "01/01/2021", "04/04/2024", "null", "null", "0005851658", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_06403060483/2020/0824/00489490011_06403060483_202008_TMV0350_20200824203108_1.xml", None, "",
          "", "2020-08-25T08:51:22.887217", "v", "202101")
      ).toDF(
        TavSchema.cod_servizio,
        TavSchema.cod_pdr,
        TavSchema.data_com_autolet_cf,
        TavSchema.data_racc,
        TavSchema.let_tot_prel,
        TavSchema.let_tot_conv,
        TavSchema.matr_mis,
        TavSchema.matr_conv,
        TavSchema.local_file,
        TavSchema.ammissibilita,
        TavSchema.piva_distr,
        TavSchema.piva_utente,
        TavSchema.d_caricamento,
        TavSchema.esito_val,
        MeasureDAO.ANNO_MESE_COL_NAME
      )
    }
  }

}

