package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.schema.measure.RglSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class RglDAOTest extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = new RglDAOMock().get("202008", "202009", getTreatment = true).cache
    result.collect().foreach(println)

    Assert.assertEquals(2, result.count())

    Assert.assertEquals("2020-08-01 00:00:00", result.filter(_.pdr == "1").first().date.get.toString("yyyy-MM-dd HH:mm:ss"))
    Assert.assertEquals("2020-12-14 05:46:40", result.filter(_.pdr == "1").first().dataCaricamento.get.toString("yyyy-MM-dd HH:mm:ss"))

    Assert.assertEquals(1, result.filter(_.pdr == "2").count)
    Assert.assertEquals("2020-08-01 00:00:00", result.filter(_.pdr == "2").first().date.get.toString("yyyy-MM-dd HH:mm:ss"))
    Assert.assertEquals("01/08/2020", result.filter(_.pdr == "2").first().date.get.toString("dd/MM/yyyy"))

    Assert.assertEquals("SK11000009549", result.filter(_.pdr == "2").first().serialNumberConv.get)
    Assert.assertEquals("15204740", result.filter(_.pdr == "1").first().serialNumberMis.get)
  }

  class RglDAOMock extends RglDAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._
      List(
        ("RGL", "1", "2020-08-01 00:00:00", "000003176", "000060435", "", "SK11000009549", "15204740", "",
          "/mnt/isilonshare1/GAS_INJ/TMG_03178060236/DISTRIBUTORE/TMG_03178060236_08526440154/2019/0218/03178060236_08526440154_201902_RGL0055_20190218152522_79.xml", "", "", "",
          "5", "2020-12-14T05:46:40.148595", "G", "082020"),
        ("RGL", "2", "01/08/2020", "000003176", "000060435", "15204740", "SK11000009549", "", "",
          "/mnt/isilonshare1/GAS_INJ/TMG_03178060236/DISTRIBUTORE/TMG_03178060236_08526440154/2019/0218/03178060236_08526440154_201902_RGL0055_20190218152522_79.xml", "", "", "",
          "5", "2020-12-14T05:46:40.148595", null, "092020"),
        ("RGL", "2", "2020-08-01 00:00:00", "000003176", "000060435", "", "", "15204740", "SK11000009549",
          "/mnt/isilonshare1/GAS_INJ/TMG_03178060236/DISTRIBUTORE/TMG_03178060236_08526440154/2019/0218/03178060236_08526440154_201902_RGL0055_20190218152522_79.xml", "", "", "",
          "5", "2020-12-14T05:46:40.148595", "Y", "102020")
      ).toDF(
        RglSchema.cod_servizio,
        RglSchema.cod_pdr,
        RglSchema.data_racc,
        RglSchema.let_tot_prel,
        RglSchema.let_tot_conv,
        RglSchema.matr_mis,
        RglSchema.matr_conv,
        RglSchema.matr_mis_giornaliere,
        RglSchema.matr_conv_giornaliere,
        RglSchema.local_file,
        RglSchema.ammissibilita,
        RglSchema.piva_distr,
        RglSchema.piva_utente,
        RglSchema.mot_rett_lett,
        RglSchema.d_caricamento,
        MeasureDAO.TRATTAMENTO,
        MeasureDAO.MESE_COMP_COL_NAME
      )
    }
  }

}
