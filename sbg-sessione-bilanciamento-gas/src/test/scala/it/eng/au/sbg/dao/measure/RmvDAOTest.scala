package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO
import it.eng.au.aggiustamentoGas.schema.measure._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class RmvDAOTest extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = RmvDaoMock.get("202008", "202101", getTreatment = false).cache

    Assert.assertEquals(5, result.count())
    //Testing MeasureDAO.getDate method
    Assert.assertEquals("01/01/2011", result.filter(_.pdr == "2").first().date.get.toString("dd/MM/yyyy")) //checking date values parsing
    Assert.assertEquals("01/01/2011", result.filter(_.pdr == "3").first().date.get.toString("dd/MM/yyyy")) //checking date values parsing
    Assert.assertEquals("07/02/2017", result.filter(_.pdr == "4").first().date.get.toString("dd/MM/yyyy")) //checking date values parsing
    Assert.assertEquals("08/02/2017", result.filter(_.pdr == "5").first().date.get.toString("dd/MM/yyyy")) //checking date values parsing
    Assert.assertTrue(result.filter(_.pdr == "6").first().date.isEmpty) //checking date values parsing
    //general mapping/conversion tests
    Assert.assertEquals("2021-03-03 03:30:05", result.filter(_.pdr == "2").first().dataCaricamento.get.toString("yyyy-MM-dd HH:mm:ss")) //checking date values parsing
    Assert.assertEquals(1, result.filter(_.pdr == "2").count) //checking pdr uniqueness
    Assert.assertEquals(780, result.filter(_.pdr == "2").first().measure.get, 0.0) //checking double values conversion
    Assert.assertEquals(false, result.filter(_.pdr == "2").first().converted.isDefined) //checking null values handling
    Assert.assertEquals(0.0, result.filter(_.pdr == "6").first().converted.get, 0.0) //checking double values conversion
  }


  /**
   * Simulating parquet data reading for unit test purpose.
   */
  object RmvDaoMock extends RmvDAOSbg {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark
    import sqlContext.implicits._
      List(
        ("RMV", "1", "07/02/2017", "07/02/2017", "71522.0", "0", "25164719", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_12883450152/DISTRIBUTORE/TMG_12883450152_12883420155/2020/0719/12883450152_12883420155_202007_RMV0400_20200719011651_13.xml", "", "",
          "", "2020-07-20T05:57:36.479608", "1", "202007"),
        ("RMV", "2", "null", "01/01/2011", "000000780", "null", "56885353", "null",
          "/mnt/isilonshare_gas/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2021/0302/01791490343_01178580997_202102_RMV_20210302200033_2_R.zip", "", "",
          "", "2021-03-03T03:30:05.332000", "2", "202012"),
        ("RMV", "3", "01/01/2011", "01/01/2011", "000000780", "null", "56885353", "null",
          "/mnt/isilonshare_gas/TMG_01791490343/DISTRIBUTORE/TMG_01791490343_01178580997/2021/0302/01791490343_01178580997_202102_RMV_20210302200033_2_R.zip", "", "",
          "", "2021-03-03T03:30:05.332000", "2", "202012"),
        ("RMV", "4", "07/02/2017", "07/02/2017", "48701.0", "0", "240990340061410052", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03479071205/DISTRIBUTORE/TMG_03479071205_02221101203/2020/0708/03479071205_02221101203_202006_RMV.0400_20200708123944_001.xml", "", "",
          "", "2020-07-09T07:35:25.323770", "1", "202012"),
        ("RMV", "5", "NULL", "08/02/2017", "1672.0", "0", "0058228408", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12300020158/2020/1105/00489490011_12300020158_202010_RMV0400_20201105113008_1.xml", "", "",
          "", "2020-11-06T15:00:44.589234", "2", "202012"),
        ("RMV", "6", "null", "   ", "875.0", "0", "ITGF030102119890", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12300020158/2020/1105/00489490011_12300020158_202010_RMV0400_20201105113008_1.xml", "", "",
          "", "2020-11-06T15:00:44.589234", "1", "202012")

      ).toDF(
        RmvSchema.cod_servizio,
        RmvSchema.cod_pdr,
        RmvSchema.data_comp,
        RmvSchema.data_prest,
        RmvSchema.let_tot_prel,
        RmvSchema.let_tot_conv,
        RmvSchema.matr_mis,
        RmvSchema.matr_conv,
        RmvSchema.local_file,
        RmvSchema.ammissibilita,
        RmvSchema.piva_distr,
        RmvSchema.piva_utente,
        RmvSchema.d_caricamento,
        RmvSchema.mot_rett_lett,
        MeasureDAO.ANNO_MESE_COL_NAME
      ).withColumn(MeasureDAO.TRATTAMENTO, lit("N"))
    }
  }

}
