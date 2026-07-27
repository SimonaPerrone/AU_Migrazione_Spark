package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.model.measure.Tml
import it.eng.au.aggiustamentoGas.schema.measure.TmlSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class TmlDAOTest extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = new TmlDAOMock().get("201708", "202009", getTreatment = false).cache

    Assert.assertEquals(1, result.count())

    Assert.assertEquals("2018-11-03 00:00:00", result.filter(_.pdr == "1").first().date.get.toString("yyyy-MM-dd HH:mm:ss"))
    Assert.assertEquals(Some('E'), result.filter(_.pdr == "1").first().asInstanceOf[Tml].readType)
  }

  class TmlDAOMock extends TmlDAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._

      List(
        ("Tml", "1", "03/11/2018", "000094011", "000095361", "8304569", "900124000178",
          "/mnt/isilonshare1/GAS_INJ/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_06655971007/2020/0617/01791490343_06655971007_202006_Tml0050_20200617120115_842.xml", "", "", "",
          "E", "SI", "1.031253", "3", "2020-06-18T04:19:41.016989", "201811")
      ).toDF(
        TmlSchema.cod_servizio,
        TmlSchema.cod_pdr,
        TmlSchema.data_racc,
        TmlSchema.let_tot_prel,
        TmlSchema.let_tot_conv,
        TmlSchema.matr_mis,
        TmlSchema.matr_conv,
        TmlSchema.local_file,
        TmlSchema.ammissibilita,
        TmlSchema.piva_distr,
        TmlSchema.piva_utente,
        TmlSchema.tipo_lettura,
        TmlSchema.val_dato,
        TmlSchema.coeff_corr,
        TmlSchema.freq_let,
        TmlSchema.d_caricamento,
        MeasureDAO.ANNO_MESE_COL_NAME
      ).withColumn(MeasureDAO.TRATTAMENTO, lit("N"))
    }
  }

}
