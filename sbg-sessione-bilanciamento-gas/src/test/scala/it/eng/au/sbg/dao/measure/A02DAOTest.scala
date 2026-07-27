package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.schema.measure._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class A02DAOTest extends EnvironmentSparkTest {

  def testGet(): Unit = {
    val result = A02DAOMock.get("201708", "202009", getTreatment = false).cache

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(0, result.filter(_.service != "A02").count)
    Assert.assertEquals(0, result.filter(_.pdr == "1").count)

    Assert.assertEquals(94011.0, result.filter(_.pdr == "5").first().measure.get, 0.0)
    Assert.assertEquals("2020-05-06 05:17:03.89", result.filter(_.pdr == "3").first().dataCaricamento.get.toString("yyyy-MM-dd hh:mm:ss.SS"))

    //Checking date conversion
    Assert.assertEquals("01/05/2021", result.filter(_.pdr == "2").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("28/04/2020", result.filter(_.pdr == "3").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("18/08/2020", result.filter(_.pdr == "4").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("01/05/2021", result.filter(_.pdr == "5").first().date.get.toString("dd/MM/yyyy"))
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

  object A02DAOMock extends A02DAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._
      List(
        ("A02", "1", "07/05/2020", "01/05/2021", "E", "000000706", "000081281", "000094011", "000095361", "0005559244", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_A02.0150_20200530171817_397.xml", "", "",
          "", "2020-05-14T05:46:22.996999", "201706"),
        ("A02", "2", "15/04/2020", "01/05/2021", "E", "000000706", "000081281", "000000706", "000081281", "0000486011", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_A02.0150_20200530171817_397.xml", "", "",
          "", "2020-04-24T04:00:51.369432", "202001"),
        ("A02", "3", "28/04/2020", "28/04/2020", "E", "000000706", "000081281", "000000710", "000081282", "0055329616", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_A02.0150_20200530171817_397.xml", "", "",
          "", "2020-05-06T05:17:03.890467", "202001"),
        ("A02", "4", "NULL", "18/08/2020", "E", "null", "null", "000000710", "000081282", "0005851658", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_A02.0150_20200530171817_397.xml", "", "",
          "", "2020-08-25T08:51:22.887217", "202001"),
        ("A02", "5", "02/01/2020", "01/05/2021", "E", "000000706", "000081281", "000094011", "null", "0084866499", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_A02.0150_20200530171817_397.xml", "", "",
          "", "2020-05-12T21:15:58.590415", "202001"),
        ("A02", "6", "", "", "E", "", "", "null", "null", "0032520730", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_A02.0150_20200530171817_397.xml", "", "",
          "", "2020-01-31T04:28:55.508716", "202001")
      ).toDF(
        A02Schema.cod_servizio,
        A02Schema.cod_pdr,
        A02Schema.data_sospensione,
        A02Schema.data_prest,
        A02Schema.tipo_lettura,
        A02Schema.segn_mis,
        A02Schema.segn_conv,
        A02Schema.let_tot_prel,
        A02Schema.let_tot_conv,
        A02Schema.matr_mis,
        A02Schema.matr_conv,
        A02Schema.local_file,
        A02Schema.ammissibilita,
        A02Schema.piva_distr,
        A02Schema.piva_utente,
        A02Schema.d_caricamento,
        MeasureDAO.ANNO_MESE_COL_NAME
      ).withColumn(MeasureDAO.TRATTAMENTO, lit("N"))
    }
  }

}
