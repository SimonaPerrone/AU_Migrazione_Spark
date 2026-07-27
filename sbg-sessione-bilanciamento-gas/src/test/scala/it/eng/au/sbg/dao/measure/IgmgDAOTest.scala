package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.Igmg
import it.eng.au.aggiustamentoGas.schema.measure._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class IgmgDAOTest extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = new IgmgDAOMock().get("201906", "201909", getTreatment = true).cache

    result.collect().foreach(println)

    Assert.assertEquals(1, result.count())

    Assert.assertEquals("2019-07-18 00:00:00", result.filter(_.pdr == "1").first().date.get.toString("yyyy-MM-dd HH:mm:ss"))
    Assert.assertEquals(Some(31834D), result.filter(_.pdr == "1").first().asInstanceOf[Igmg].pre.measure)
    Assert.assertEquals(None, result.filter(_.pdr == "1").first().asInstanceOf[Igmg].pre.converted)
    Assert.assertEquals(Some(1.019078D), result.filter(_.pdr == "1").first().asInstanceOf[Igmg].pre.coefCorr)
  }

  class IgmgDAOMock extends IgmgDAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._

      List(
        ("Igmg", "1", "18/07/2019", "000031834",
          null: String, "28292546", null: String, "E", "1.019078",
          "000000000", null: String, "MTSB036803170771", null: String, "1",
          "/mnt/isilonshare1/GAS_INJ/TMG_04152790962/DISTRIBUTORE/TMG_04152790962_12300020158/2020/0522/04152790962_12300020158_201907_Igmg0306_20200522101145_78.xml", "", "", "",
          "3", null: String, "2020-05-23T06:17:47.824146", "201907")
      ).toDF(
        IgmgSchema.cod_flusso,
        IgmgSchema.cod_pdr,
        IgmgSchema.data_misura,
        IgmgSchema.let_misuratore_pre_int,
        IgmgSchema.let_correttore_pre_int,
        IgmgSchema.matr_mis_pre_int,
        IgmgSchema.matr_conv_pre_int,
        IgmgSchema.tipo_let,
        IgmgSchema.coeff_corr_pre_int,
        IgmgSchema.let_misuratore_post_int,
        IgmgSchema.let_correttore_post_int,
        IgmgSchema.matr_mis_post_int,
        IgmgSchema.matr_conv_post_int,
        IgmgSchema.coeff_corr_post_int,
        IgmgSchema.local_file,
        IgmgSchema.ammissibilita,
        IgmgSchema.piva_distr,
        IgmgSchema.piva_utente,
        IgmgSchema.cau_int_mis,
        IgmgSchema.cau_int_cor,
        IgmgSchema.d_caricamento,
        MeasureDAO.ANNO_MESE_COL_NAME
      ).withColumn(MeasureDAO.TRATTAMENTO, lit("N"))
    }
  }

}
