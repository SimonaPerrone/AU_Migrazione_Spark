package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.schema.measure._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class A40rDAOTest extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = A40rDaoMock.get("202008", "202101", getTreatment = false).cache

    Assert.assertEquals(5, result.count())
    Assert.assertEquals(0, result.filter(_.pdr == "5").count)
    //Testing MeasureDAO.getDate method
    Assert.assertEquals("07/02/2017", result.filter(_.pdr == "1").first().date.get.toString("dd/MM/yyyy")) //checking date values parsing
    Assert.assertEquals(None, result.filter(_.pdr == "2").first().date) //checking date values parsing
    Assert.assertEquals("12/03/2019", result.filter(_.pdr == "3").first().date.get.toString("dd/MM/yyyy")) //checking date values parsing
    Assert.assertFalse(result.filter(_.pdr == "4").first().date.isDefined) //checking date values parsing
    Assert.assertTrue(result.filter(_.pdr == "6").first().date.isEmpty) //checking date values parsing
    //general mapping/conversion tests
    Assert.assertEquals("2021-03-03 03:30:05", result.filter(_.pdr == "2").first().dataCaricamento.get.toString("yyyy-MM-dd HH:mm:ss")) //checking date values parsing
    Assert.assertEquals(1, result.filter(_.pdr == "2").count) //checking pdr uniqueness
    Assert.assertEquals(780.0, result.filter(_.pdr == "2").first().measure.get, 0.0) //checking double values conversion
    Assert.assertFalse(result.filter(_.pdr == "2").first().converted.isDefined) //checking null values handling
    Assert.assertEquals(0.0, result.filter(_.pdr == "6").first().converted.get, 0.0) //checking double values conversion
  }


  /**
   * Simulating parquet data reading for unit test purpose.
   */
  object A40rDaoMock extends A40rDAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._
      List(
        ("A40r", "1", "07/02/2017", "000002354", "0", "25164719", "null",
          "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_02030110692/2021/0303/06724610966_02030110692_202102_A40R_20210303121616_00_R.zip", "", "",
          "", "2020-07-20T05:57:36.479608", "1", "202101"),
        ("A40r", "2", "null", "000000780", "null", "0016048901", "4408459",
          "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_02030110692/2021/0303/06724610966_02030110692_202102_A40R_20210303121616_00_R.zip", "", "",
          "", "2021-03-03T03:30:05.332000", "1", "202012"),
        ("A40r", "3", "12/03/2019", "000000780", "null", "56885353", "null",
          "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_02030110692/2021/0303/06724610966_02030110692_202102_A40R_20210303121616_00_R.zip", "", "",
          "", "2021-03-03T03:30:05.332000", "1", "202012"),
        ("A40r", "4", "", "000000048701.0", "0", "240990340061410052", "null",
          "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_02030110692/2021/0303/06724610966_02030110692_202102_A40R_20210303121616_00_R.zip", "", "",
          "", "2020-07-09T07:35:25.323770", "1", "202012"),
        ("A40r", "6", "   ", "00000875", "0", "ITGF030102119890", "null",
          "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_02030110692/2021/0303/06724610966_02030110692_202102_A40R_20210303121616_00_R.zip", "", "",
          "", "2020-11-06T15:00:44.589234", "1", "202012")

      ).toDF(
        A40rSchema.cod_flusso,
        A40rSchema.cod_pdr,
        A40rSchema.data_prest,
        A40rSchema.let_tot_prel,
        A40rSchema.let_tot_conv,
        A40rSchema.matr_mis,
        A40rSchema.matr_conv,
        A40rSchema.local_file,
        A40rSchema.ammissibilita,
        A40rSchema.piva_distr,
        A40rSchema.piva_utente,
        A40rSchema.d_caricamento,
        A40rSchema.mot_ret_lett,
        MeasureDAO.ANNO_MESE_COL_NAME
      ).withColumn(MeasureDAO.TRATTAMENTO, lit("N"))
    }
  }

}
