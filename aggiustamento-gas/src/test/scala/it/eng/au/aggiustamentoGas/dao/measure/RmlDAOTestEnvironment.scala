package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.schema.measure.RmlSchema._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class RmlDAOTestEnvironment extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = new RmlDAOMock().get("202008", "202009", getTreatment = false).cache

    Assert.assertEquals(2, result.count())

    Assert.assertEquals("2020-08-01 00:00:00", result.filter(_.pdr == "1").first().date.get.toString("yyyy-MM-dd HH:mm:ss"))
    Assert.assertEquals("2020-12-14 05:46:40", result.filter(_.pdr == "1").first().dataCaricamento.get.toString("yyyy-MM-dd HH:mm:ss"))

    Assert.assertEquals(1, result.filter(_.pdr == "2").count)
  }

  class RmlDAOMock extends RmlDAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("RML", "1", "01/08/2020", "000003176", "000060435", "15204740", "SK11000009549",
          "/mnt/isilonshare1/GAS_INJ/TMG_03178060236/DISTRIBUTORE/TMG_03178060236_08526440154/2019/0218/03178060236_08526440154_201902_Rml0055_20190218152522_79.xml", "", "", "",
          "5", "1", "T", "2020-12-14T05:46:40.148595", "202008"),
        ("RML", "2", "01/08/2020", "000003176", "000060435", "15204740", "SK11000009549",
          "/mnt/isilonshare1/GAS_INJ/TMG_03178060236/DISTRIBUTORE/TMG_03178060236_08526440154/2019/0218/03178060236_08526440154_201902_Rml0055_20190218152522_79.xml", "", "", "",
          "5", "1", "T", "2020-12-14T05:46:40.148595", "202009"),
        ("RML", "3", "01/08/2020", "000003176", "000060435", "15204740", "SK11000009549",
          "/mnt/isilonshare1/GAS_INJ/TMG_03178060236/DISTRIBUTORE/TMG_03178060236_08526440154/2019/0218/03178060236_08526440154_201902_Rml0055_20190218152522_79.xml", "", "","",
          "5", "1", "T", "2020-12-14T05:46:40.148595", "202010")
      ).toDF(
        cod_servizio,
        cod_pdr,
        data_racc,
        let_tot_prel,
        let_tot_conv,
        matr_mis,
        matr_conv,
        local_file,
        ammissibilita,
        piva_distr,
        piva_utente,
        mot_rett_lett,
        freq_let,
        tipo_rettifica,
        d_caricamento,
        ANNO_MESE_COL_NAME
      )
    }
  }

}
