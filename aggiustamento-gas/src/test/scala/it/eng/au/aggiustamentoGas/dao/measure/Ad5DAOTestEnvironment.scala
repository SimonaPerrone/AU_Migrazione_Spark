package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.schema.measure.Ad5Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class Ad5DAOTestEnvironment extends EnvironmentSparkTest {

  def testGet(): Unit = {
    val result = Ad5DAOMock.get("201708", "202009", getTreatment = false).cache

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(0, result.filter(_.service != "AD5").count)
    Assert.assertEquals(0, result.filter(_.pdr == "1").count)

    Assert.assertEquals(94011.0, result.filter(_.pdr == "5").first().measure.get, 0.0)
    Assert.assertEquals("2020-05-06 05:17:03.89", result.filter(_.pdr == "3").first().dataCaricamento.get.toString("yyyy-MM-dd hh:mm:ss.SS"))

    //Checking date conversion
    Assert.assertEquals("01/05/2021", result.filter(_.pdr == "2").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertFalse(result.filter(_.pdr == "3").first().date.isDefined)
    Assert.assertEquals("18/08/2020", result.filter(_.pdr == "4").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("01/05/2021", result.filter(_.pdr == "5").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertFalse(result.filter(_.pdr == "6").first().date.isDefined)

    //Checking measure conversion
    Assert.assertFalse(result.filter(_.pdr == "2").first().measure.isDefined)
    Assert.assertEquals(710.0, result.filter(_.pdr == "3").first().measure.get, 0.0)
    Assert.assertEquals(710.0, result.filter(_.pdr == "4").first().measure.get, 0.0)
    Assert.assertEquals(94011.0, result.filter(_.pdr == "5").first().measure.get, 0.0)
    Assert.assertFalse(result.filter(_.pdr == "6").first().measure.isDefined)

    //Checking converted conversion
    Assert.assertFalse(result.filter(_.pdr == "2").first().converted.isDefined)
    Assert.assertEquals(81282.0, result.filter(_.pdr == "3").first().converted.get, 0.0)
    Assert.assertEquals(81282.0, result.filter(_.pdr == "4").first().converted.get, 0.0)
    Assert.assertFalse(result.filter(_.pdr == "5").first().converted.isDefined)
    Assert.assertFalse(result.filter(_.pdr == "6").first().converted.isDefined)

  }

  object Ad5DAOMock extends Ad5DAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._
      List(
        ("AD5", "1", "01/05/2021", "S", "000094011", "000095361", "0005559244", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_Ad5.0150_20200530171817_397.xml", "", "",
          "", "2020-05-14T05:46:22.996999", "201706"),
        ("AD5", "2", "01/05/2021", "E", "null", "null", "0000486011", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_Ad5.0150_20200530171817_397.xml", "", "",
          "", "2020-04-24T04:00:51.369432", "202001"),
        ("AD5", "3", null, "E", "000000710", "000081282", "0055329616", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_Ad5.0150_20200530171817_397.xml", "", "",
          "", "2020-05-06T05:17:03.890467", "202001"),
        ("AD5", "4", "18/08/2020", "E", "000000710", "000081282", "0005851658", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_Ad5.0150_20200530171817_397.xml", "", "",
          "", "2020-08-25T08:51:22.887217", "202001"),
        ("AD5", "5", "01/05/2021", "E", "000094011", "null", "0084866499", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_Ad5.0150_20200530171817_397.xml", "", "",
          "", "2020-05-12T21:15:58.590415", "202001"),
        ("AD5", "6", "", "E", "null", "null", "0032520730", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03757660968/DISTRIBUTORE/TMG_03757660968_03757680966/2020/0611/03757660968_03757680966_Ad5.0150_20200530171817_397.xml", "", "",
          "", "2020-01-31T04:28:55.508716", "202001")
      ).toDF(
        Ad5Schema.cod_flusso,
        Ad5Schema.cod_pdr,
        Ad5Schema.data_prest,
        Ad5Schema.tipo_lettura,
        Ad5Schema.let_tot_prel,
        Ad5Schema.let_tot_conv,
        Ad5Schema.matr_mis,
        Ad5Schema.matr_conv,
        Ad5Schema.local_file,
        Ad5Schema.ammissibilita,
        Ad5Schema.piva_distr,
        Ad5Schema.piva_utente,
        Ad5Schema.d_caricamento,
        MeasureDAO.ANNO_MESE_COL_NAME
      )
    }
  }

}
