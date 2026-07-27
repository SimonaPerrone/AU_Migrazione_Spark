package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.schema.measure._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class D01DAOTest extends EnvironmentSparkTest {

  def testGet(): Unit = {
    val result = D01DAOMock.get("201708", "202009", getTreatment = false).cache

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(0, result.filter(_.service != "D01").count)
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

    //Checking outcome conversion
//    Assert.assertEquals(Some('1'), result.filter(_.pdr == "2").first().asInstanceOf[D01].outcome)
//    Assert.assertEquals(Some('1'), result.filter(_.pdr == "3").first().asInstanceOf[D01].outcome)
//    Assert.assertEquals(Some('1'), result.filter(_.pdr == "4").first().asInstanceOf[D01].outcome)
//    Assert.assertEquals(Some('1'), result.filter(_.pdr == "5").first().asInstanceOf[D01].outcome)
//    Assert.assertFalse(result.filter(_.pdr == "6").first().outcome.isDefined)
  }

  object D01DAOMock extends D01DAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._
      List(
        ("D01", "1", "07/05/2020", "01/05/2021", "S", "000000706", "000081281", "000094011", "000095361", "0005559244", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_D01.0150_20200530171817_397.xml", "", "",
          "", "2020-05-14T05:46:22.996999", "", "201706"),
        ("D01", "2", "15/04/2020", "01/05/2021", "E", "000000706", "000081281", "000000706", "000081281", "0000486011", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_D01.0150_20200530171817_397.xml", "", "",
          "", "2020-04-24T04:00:51.369432", "1", "202001"),
        ("D01", "3", "28/04/2020", "28/04/2020", "E", "000000706", "000081281", "000000710", "000081282", "0055329616", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_D01.0150_20200530171817_397.xml", "", "",
          "", "2020-05-06T05:17:03.890467", "1", "202001"),
        ("D01", "4", "NULL", "18/08/2020", "E", "null", "null", "000000710", "000081282", "0005851658", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_D01.0150_20200530171817_397.xml", "", "",
          "", "2020-08-25T08:51:22.887217", "1", "202001"),
        ("D01", "5", "02/01/2020", "01/05/2021", "E", "000000706", "000081281", "000094011", "null", "0084866499", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_D01.0150_20200530171817_397.xml", "", "",
          "", "2020-05-12T21:15:58.590415", "1", "202001"),
        ("D01", "6", "", "", "E", "", "", "null", "null", "0032520730", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_D01.0150_20200530171817_397.xml", "", "",
          "", "2020-01-31T04:28:55.508716", "NULL", "202001")
      ).toDF(
        D01Schema.cod_servizio,
        D01Schema.cod_pdr,
        D01Schema.data_disattivazione,
        D01Schema.data_prest,
        D01Schema.tipo_lettura,
        D01Schema.segn_mis,
        D01Schema.segn_conv,
        D01Schema.let_tot_prel,
        D01Schema.let_tot_conv,
        D01Schema.matr_mis,
        D01Schema.matr_conv,
        D01Schema.local_file,
        D01Schema.ammissibilita,
        D01Schema.piva_distr,
        D01Schema.piva_utente,
        D01Schema.d_caricamento,
        D01Schema.esito,
        MeasureDAO.ANNO_MESE_COL_NAME
      ).withColumn(MeasureDAO.TRATTAMENTO, lit("N"))
    }
  }

}
