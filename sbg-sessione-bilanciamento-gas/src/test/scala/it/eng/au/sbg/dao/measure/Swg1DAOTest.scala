package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.model.measure.Swg1
import it.eng.au.aggiustamentoGas.schema.measure.Swg1Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class Swg1DAOTest extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = new Swg1DAOMock().get("201708", "202009", getTreatment = false).cache

    Assert.assertEquals(7, result.count())

    Assert.assertEquals("2018-11-03 00:00:00", result.filter(_.pdr == "1").first().date.get.toString("yyyy-MM-dd HH:mm:ss"))
    Assert.assertEquals(Some('E'), result.filter(_.pdr == "1").first().asInstanceOf[Swg1].readType)

    Assert.assertEquals("03/11/2018", result.filter(_.pdr == "1").first().asInstanceOf[Swg1].date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("03/11/2018", result.filter(_.pdr == "2").first().asInstanceOf[Swg1].date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("03/11/2018", result.filter(_.pdr == "3").first().asInstanceOf[Swg1].date.get.toString("dd/MM/yyyy"))
    Assert.assertFalse(result.filter(_.pdr == "6").first().asInstanceOf[Swg1].date.isDefined)

    Assert.assertEquals(94011.0, result.filter(_.pdr == "1").first().asInstanceOf[Swg1].measure.get, 0.0)
    Assert.assertEquals(94011.0, result.filter(_.pdr == "2").first().asInstanceOf[Swg1].measure.get, 0.0)
    Assert.assertEquals(94010.0, result.filter(_.pdr == "3").first().asInstanceOf[Swg1].measure.get, 0.0)
    Assert.assertEquals(94012.0, result.filter(_.pdr == "4").first().asInstanceOf[Swg1].measure.get, 0.0)
    Assert.assertEquals(94011.0, result.filter(_.pdr == "5").first().asInstanceOf[Swg1].measure.get, 0.0)
    Assert.assertFalse(result.filter(_.pdr == "6").first().asInstanceOf[Swg1].measure.isDefined)

    Assert.assertEquals(95368.0, result.filter(_.pdr == "1").first().asInstanceOf[Swg1].converted.get, 0.0)
    Assert.assertEquals(95368.0, result.filter(_.pdr == "2").first().asInstanceOf[Swg1].converted.get, 0.0)
    Assert.assertEquals(95361.0, result.filter(_.pdr == "3").first().asInstanceOf[Swg1].converted.get, 0.0)
    Assert.assertFalse(result.filter(_.pdr == "4").first().asInstanceOf[Swg1].converted.isDefined)
    Assert.assertFalse(result.filter(_.pdr == "5").first().asInstanceOf[Swg1].converted.isDefined)
    Assert.assertFalse(result.filter(_.pdr == "6").first().asInstanceOf[Swg1].converted.isDefined)


  }

  class Swg1DAOMock extends Swg1DAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._

      List(
        ("Swg1", "0350", "1", "03/11/2018", "null", "000094011", "000095368", "null", "null", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", null, "", "",
          "E", "2020-06-18T04:19:41.016989", "201811"),
        ("Swg1", "0350", "2", "03/11/2018", "null", "000094011", "000095368", "null", "null", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", null, "", "",
          "E", "2020-06-18T04:19:41.016989", "201811"),
        ("SWG1", "", "3", "03/11/2018", "null", "000094010", "000095361", "null", "null", "null", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", "NOTNULL", "", "",
          "E", "2020-06-18T04:19:41.016989", "201811"),
        ("Swg1", "0350", "4", "03/11/2018", "null", "000094012", "NULL", "NULL", "null", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", null, "", "",
          "E", "2020-06-18T04:19:41.016989", "201811"),
        ("Sw1", "0350", "5", "null", "03/11/2018", "000094011", "NULL", "000094011", "NULL", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", null, "", "",
          "E", "2020-06-18T04:19:41.016989", "201811"),
        ("Sw1", "0350", "6", "null", "null", "NULL", "    null  ", "NULL", "    null  ", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", null, "", "",
          "E", "2020-06-18T04:19:41.016989", "201811"),
        ("SW1", "0351", "7", "null", "03/11/2018", "NULL", "    null  ", "NULL", "    null  ", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", null, "", "",
          "E", "2020-06-18T04:19:41.016989", "201811")
      ).toDF(
        Swg1Schema.cod_servizio,
        Swg1Schema.cod_flusso,
        Swg1Schema.cod_pdr,
        Swg1Schema.data_prest,
        Swg1Schema.data_deco_switch,
        Swg1Schema.let_tot_prel,
        Swg1Schema.let_tot_conv,
        Swg1Schema.segn_mis_sost,
        Swg1Schema.segn_conv,
        Swg1Schema.matr_mis,
        Swg1Schema.matr_conv,
        Swg1Schema.local_file,
        Swg1Schema.ammissibilita,
        Swg1Schema.piva_distr,
        Swg1Schema.piva_utente,
        Swg1Schema.tipo_lettura,
        Swg1Schema.d_caricamento,
        MeasureDAO.ANNO_MESE_COL_NAME
      ).withColumn(MeasureDAO.TRATTAMENTO, lit("N"))
    }
  }

}
