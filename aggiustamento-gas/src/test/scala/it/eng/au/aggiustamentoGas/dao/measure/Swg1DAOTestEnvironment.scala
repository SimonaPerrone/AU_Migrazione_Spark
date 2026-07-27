package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.model.measure.Swg1
import it.eng.au.aggiustamentoGas.schema.measure.Swg1Schema._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class Swg1DAOTestEnvironment extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = new Swg1DAOMock().get("201708", "202009", getTreatment = false).cache

    Assert.assertEquals(6, result.count())

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

  class Swg1DAOMock extends Swg1DAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
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
        ("Sw1", "0350", "5", "null", "03/11/2018", "null", "NULL", "000094011", "NULL", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", null, "", "",
          "E", "2020-06-18T04:19:41.016989", "201811"),
        ("Sw1", "0350", "6", "null", "null", "NULL", "    null  ", "NULL", "    null  ", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", null, "", "",
          "E", "2020-06-18T04:19:41.016989", "201811"),
        ("SW1", "0351", "7", "null", "03/11/2018", "NULL", "    null  ", "NULL", "    null  ", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Swg10050_20200617120115_842.xml", null, "", "",
          "E", "2020-06-18T04:19:41.016989", "201811")
      ).toDF(
        cod_servizio,
        cod_flusso,
        cod_pdr,
        data_prest,
        data_deco_switch,
        let_tot_prel,
        let_tot_conv,
        segn_mis_sost,
        segn_conv,
        matr_mis,
        matr_conv,
        local_file,
        ammissibilita,
        piva_distr,
        piva_utente,
        tipo_lettura,
        d_caricamento,
        ANNO_MESE_COL_NAME
      )
    }
  }

}
