package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.Im1
import it.eng.au.aggiustamentoGas.schema.measure.Im1Schema._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class Im1DAOTestEnvironment extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = new Im1DAOMock().get("201906", "201909", getTreatment = false).cache

    result.collect().foreach(println)

    Assert.assertEquals(1, result.count())

    Assert.assertEquals("2019-07-18 00:00:00", result.filter(_.pdr == "1").first().date.get.toString("yyyy-MM-dd HH:mm:ss"))
    Assert.assertEquals(Some(31834D), result.filter(_.pdr == "1").first().asInstanceOf[Im1].pre.measure)
    Assert.assertEquals(None, result.filter(_.pdr == "1").first().asInstanceOf[Im1].pre.converted)
    Assert.assertEquals(Some(1.019078D), result.filter(_.pdr == "1").first().asInstanceOf[Im1].pre.coefCorr)
  }

  class Im1DAOMock extends Im1DAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("IM1", "1", "18/07/2019", "000031834",
          null: String, "28292546", null: String, null: String, "1.019078",
          "000000000", null: String, "MTSB036803170771", null: String, "1",
          "/mnt/isilonshare1/GAS_INJ/TMG_04152790962/DISTRIBUTORE/TMG_04152790962_12300020158/2020/0522/04152790962_12300020158_201907_IM10306_20200522101145_78.xml", "", "", "",
          "3", null: String, "2020-05-23T06:17:47.824146", "201907")
      ).toDF(
        cod_servizio,
        cod_pdr,
        data_esec_int,
        PRE_let_misuratore,
        PRE_let_correttore,
        PRE_matr_mis,
        PRE_matr_conv,
        PRE_tipo_mis,
        PRE_coeff_corr,
        POST_let_misuratore,
        POST_let_correttore,
        POST_matr_mis,
        POST_matr_conv,
        POST_coeff_corr,
        local_file,
        ammissibilita,
        piva_distr,
        piva_utente,
        cau_int_mis,
        cau_int_cor,
        d_caricamento,
        ANNO_MESE_COL_NAME
      )
    }
  }

}
