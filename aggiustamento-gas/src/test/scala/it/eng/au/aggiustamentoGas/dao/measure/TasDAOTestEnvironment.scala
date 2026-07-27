package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.measure.Tas
import it.eng.au.aggiustamentoGas.schema.measure.TasSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class TasDAOTestEnvironment extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = TasDAOMock.get("202101", "202101", getTreatment = false)
    Assert.assertEquals(4, result.count)
    Assert.assertEquals(2540, result.filter(_.pdr == "1").first().measure.get, 0.0)
    Assert.assertTrue(result.filter(_.pdr == "1").first().converted.isEmpty)
    Assert.assertEquals("2020-05-14 05:46:22.99", result.filter(_.pdr == "1").first().dataCaricamento.get.toString("yyyy-MM-dd HH:mm:ss.SS"))

    Assert.assertEquals('V', result.filter(_.pdr == "1").first().asInstanceOf[Tas].outcome.get)
    Assert.assertEquals('V', result.filter(_.pdr == "2").first().asInstanceOf[Tas].outcome.get)
    Assert.assertEquals('V', result.filter(_.pdr == "3").first().asInstanceOf[Tas].outcome.get)
    Assert.assertEquals('V', result.filter(_.pdr == "4").first().asInstanceOf[Tas].outcome.get)
    Assert.assertEquals(0, result.filter(_.pdr == "5").count)

    Assert.assertEquals("02/02/2022", result.filter(_.pdr == "2").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("01/01/2021", result.filter(_.pdr == "3").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("04/04/2024", result.filter(_.pdr == "4").first().date.get.toString("dd/MM/yyyy"))

  }

  object TasDAOMock extends TasDAO {
    override def readParquet: DataFrame = {
      val sqlCtx = Environment.getSpark.sqlContext
      import sqlCtx.implicits._
      // pdr==1 is an unmodified sample from parquet. Note that it has almost all fields to null. 
      // All the others are filled with manually forged data to complete test
      List(
        ("TAS", "1", "02/04/2020", "null", "000002540", "null", "0005559244", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03679070288/2020/0513/00489490011_03679070288_202005_TMV0350_20200513203503_1.xml", Option(""), "",
          "", "2020-05-14T05:46:22.996999", "V", "202101"),
        ("TAS", "2", "02/02/2022", "null", "000000967", "null", "0000486011", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01178580997/2020/0423/00489490011_01178580997_202004_TMV0350_20200423203252_1.xml", Option(""), "",
          "", "2020-04-24T04:00:51.369432", "V", "202101"),
        ("TAS", "3", "2021/03/03", "01/01/2021", "000011449", "null", "0055329616", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12300020158/2020/0505/00489490011_12300020158_202004_TMV0350_20200505203351_34.xml", Option(""), "",
          "", "2020-05-06T05:17:03.890467", "V", "202101"),
        ("TAS", "4", "01/01/2021", "04/04/2024", "000002041", "null", "0005851658", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_06403060483/2020/0824/00489490011_06403060483_202008_TMV0350_20200824203108_1.xml", Option(""), "",
          "", "2020-08-25T08:51:22.887217", " V  ", "202101"),
        ("TAS", "5", "01/01/2021", "04/04/2024", "000002041", "null", "0005851658", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_06403060483/2020/0824/00489490011_06403060483_202008_TMV0350_20200824203108_1.xml", None, "",
          "", "2020-08-25T08:51:22.887217", " V  ", "202101")
      ).toDF(
        TasSchema.cod_servizio,
        TasSchema.cod_pdr,
        TasSchema.data_com_autolet_cf,
        TasSchema.data_racc,
        TasSchema.let_tot_prel,
        TasSchema.let_tot_conv,
        TasSchema.matr_mis,
        TasSchema.matr_conv,
        TasSchema.local_file,
        TasSchema.ammissibilita,
        TasSchema.piva_distr,
        TasSchema.piva_utente,
        TasSchema.d_caricamento,
        TasSchema.esito_val,
        MeasureDAO.ANNO_MESE_COL_NAME
      )
    }
  }

}

