package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.schema.measure._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class TmvDAOTest extends EnvironmentSparkTest {

  def testGet(): Unit = {
    val result = TmvDAOMock.get("201708", "202009", getTreatment = false).cache

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(0, result.filter(_.service != "TMV").count)
    Assert.assertEquals(0, result.filter(_.pdr == "1").count)

    Assert.assertEquals(94011.0, result.filter(_.pdr == "5").first().measure.get, 0.0)

    Assert.assertEquals("2020-05-06 05:17:03.89", result.filter(_.pdr == "3").first().dataCaricamento.get.toString("yyyy-MM-dd hh:mm:ss.SS"))

    //Checking date conversion
    Assert.assertEquals("15/04/2020", result.filter(_.pdr == "2").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("28/04/2020", result.filter(_.pdr == "3").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("18/08/2020", result.filter(_.pdr == "4").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("02/01/2020", result.filter(_.pdr == "5").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertFalse(result.filter(_.pdr == "6").first().date.isDefined)

    //Checking measure conversion
    Assert.assertEquals(706.0, result.filter(_.pdr == "2").first().measure.get, 0.0)
    Assert.assertEquals(710.0, result.filter(_.pdr == "3").first().measure.get, 0.0)
    Assert.assertEquals(710.0, result.filter(_.pdr == "4").first().measure.get, 0.0)
    Assert.assertFalse(result.filter(_.pdr == "6").first().measure.isDefined)

    //Checking converted conversion
    Assert.assertEquals(81281.0, result.filter(_.pdr == "2").first().converted.get, 0.0)
    Assert.assertEquals(81282.0, result.filter(_.pdr == "3").first().converted.get, 0.0)
    Assert.assertEquals(81282.0, result.filter(_.pdr == "4").first().converted.get, 0.0)
    Assert.assertFalse(result.filter(_.pdr == "6").first().converted.isDefined)
  }

  object TmvDAOMock extends TmvDAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._
      List(
        ("TMV", "1", "07/05/2020", "01/05/2021", "S", "000000706", "000081281", "000094011", "000095361", "000094011", "000095361", "0005559244", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03679070288/2020/0513/00489490011_03679070288_202005_TMV0350_20200513203503_1.xml", "", "",
          "", "2020-05-14T05:46:22.996999", "201706"),
        ("TMV", "2", "15/04/2020", "15/04/2020", "E", "000000706", "000081281", "000000706", "000081281", "null", "null", "0000486011", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01178580997/2020/0423/00489490011_01178580997_202004_TMV0350_20200423203252_1.xml", "", "",
          "", "2020-04-24T04:00:51.369432", "202001"),
        ("TMV", "3", "28/04/2020", "28/04/2020", "E", "000000706", "000081281", "000000710", "000081282", "000000710", "000081282", "0055329616", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12300020158/2020/0505/00489490011_12300020158_202004_TMV0350_20200505203351_34.xml", "", "",
          "", "2020-05-06T05:17:03.890467", "202001"),
        ("TMV", "4", "NULL", "18/08/2020", "E", "null", "null", "000000710", "000081282",  "000000710", "000081282", "0005851658", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_06403060483/2020/0824/00489490011_06403060483_202008_TMV0350_20200824203108_1.xml", "", "",
          "", "2020-08-25T08:51:22.887217", "202001"),
        ("TMV", "5", "02/01/2020", "02/01/2020", "E", "000000706", "000081281", "000094011", "null", "000094011", "null", "0084866499", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_06655971007/2020/0110/00489490011_06655971007_202001_TMV0350_20200102000000_50.xml", "", "",
          "", "2020-05-12T21:15:58.590415", "202001"),
        ("TMV", "6", "", "", "E", "", "", "null", "null", "null", "null", "0032520730", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_06655971007/2020/0130/00489490011_06655971007_202001_TMV0350_20200130203318_131.xml", "", "",
          "", "2020-01-31T04:28:55.508716", "202001")
      ).toDF(
        TmvSchema.cod_servizio,
        TmvSchema.cod_pdr,
        TmvSchema.data_att_contr,
        TmvSchema.data_prest,
        TmvSchema.tipo_lettura,
        TmvSchema.segn_mis_sost,
        TmvSchema.segn_conv,
        TmvSchema.let_tot_prel,
        TmvSchema.let_tot_conv,
        TmvSchema.segn_mis_eff,
        TmvSchema.segn_conv_eff,
        TmvSchema.matr_mis,
        TmvSchema.matr_conv,
        TmvSchema.local_file,
        TmvSchema.ammissibilita,
        TmvSchema.piva_distr,
        TmvSchema.piva_utente,
        TmvSchema.d_caricamento,
        MeasureDAO.ANNO_MESE_COL_NAME
      ).withColumn(MeasureDAO.TRATTAMENTO, lit("N"))
    }
  }

}
