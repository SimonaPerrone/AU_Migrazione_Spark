package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.schema.measure._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class FddDAOTest extends EnvironmentSparkTest {

  def testGet(): Unit = {
    val result = FddDAOMock.get("201708", "202009", getTreatment = false).cache

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(0, result.filter(_.service != "FDD").count)
    Assert.assertEquals(0, result.filter(_.pdr == "1").count)

    Assert.assertEquals("2020-05-06 05:17:03.89", result.filter(_.pdr == "3").first().dataCaricamento.get.toString("yyyy-MM-dd hh:mm:ss.SS"))

    //Checking date conversion
    Assert.assertEquals("01/05/2021", result.filter(_.pdr == "2").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals(None, result.filter(_.pdr == "3").first().date)
    Assert.assertEquals("18/08/2020", result.filter(_.pdr == "4").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("01/05/2021", result.filter(_.pdr == "5").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertFalse(result.filter(_.pdr == "6").first().date.isDefined)

    //Checking measure conversion
    Assert.assertEquals(None, result.filter(_.pdr == "2").first().measure)
    Assert.assertEquals(710.0, result.filter(_.pdr == "3").first().measure.get, 0.0)
    Assert.assertEquals(710.0, result.filter(_.pdr == "4").first().measure.get, 0.0)
    Assert.assertEquals(94011.0, result.filter(_.pdr == "5").first().measure.get, 0.0)

    Assert.assertFalse(result.filter(_.pdr == "6").first().measure.isDefined)

    //Checking converted conversion
    Assert.assertEquals(81281.0, result.filter(_.pdr == "2").first().converted.get, 0.0)
    Assert.assertEquals(81282.0, result.filter(_.pdr == "3").first().converted.get, 0.0)
    Assert.assertEquals(81282.0, result.filter(_.pdr == "4").first().converted.get, 0.0)
    Assert.assertFalse(result.filter(_.pdr == "6").first().converted.isDefined)
  }

  object FddDAOMock extends FddDAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._
      List(
        ("FDD", "1", "01/05/2021", "01/05/2021", "S", "000094011", "000095361", "000094011", "000095361", "0005559244", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_06655971007/2020/0407/06724610966_06655971007_202004_Fdd0350_20200407205645_01.xml", "", "",
          "", "2020-05-14T05:46:22.996999", "201706"),
        ("FDD", "2", "01/05/2021", "01/05/2021", "E", "null", "0081281",  "null", "0081281", "0000486011", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_06655971007/2020/0407/06724610966_06655971007_202004_Fdd0350_20200407205645_01.xml", "", "",
          "", "2020-04-24T04:00:51.369432", "202001"),
        ("FDD", "3", null, null, "E", "000000710", "000081282", "000000710", "000081282", "0055329616", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_06655971007/2020/0407/06724610966_06655971007_202004_Fdd0350_20200407205645_01.xml", "", "",
          "", "2020-05-06T05:17:03.890467", "202001"),
        ("FDD", "4", "18/08/2020", "18/08/2020", "E", "000000710", "000081282",  "000000710", "000081282", "0005851658", "",
          "/mnt/isilonshare1/GAS_INJ/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_06655971007/2020/0407/06724610966_06655971007_202004_Fdd0350_20200407205645_01.xml", "", "",
          "", "2020-08-25T08:51:22.887217", "202001"),
        ("FDD", "5", "01/05/2021", "01/05/2021", "E", "000094011", "null", "000094011", "null", "0084866499", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_06655971007/2020/0407/06724610966_06655971007_202004_Fdd0350_20200407205645_01.xml", "", "",
          "", "2020-05-12T21:15:58.590415", "202001"),
        ("FDD", "6", "",  "", "E", "null", "null", "null", "null", "0032520730", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_06655971007/2020/0407/06724610966_06655971007_202004_Fdd0350_20200407205645_01.xml", "", "",
          "", "2020-01-31T04:28:55.508716", "202001")
      ).toDF(
        FddSchema.cod_servizio,
        FddSchema.cod_pdr,
        FddSchema.data_prestdata_mis_eff,
        FddSchema.data_prest,
        FddSchema.tipo_lettura,
        FddSchema.let_tot_prel,
        FddSchema.let_tot_conv,
        FddSchema.let_tot_prelsegn_mis_eff,
        FddSchema.let_tot_convsegn_conv_eff,
        FddSchema.matr_mis,
        FddSchema.matr_conv,
        FddSchema.local_file,
        FddSchema.ammissibilita,
        FddSchema.piva_distr,
        FddSchema.piva_utente,
        FddSchema.d_caricamento,
        MeasureDAO.ANNO_MESE_COL_NAME
      ).withColumn(MeasureDAO.TRATTAMENTO, lit("N"))
    }
  }

}
