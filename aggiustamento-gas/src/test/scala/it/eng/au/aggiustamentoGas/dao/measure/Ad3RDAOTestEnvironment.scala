package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.schema.measure.Ad3rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class Ad3RDAOTestEnvironment extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = Ad3rDaoMock.get("202008", "202101", getTreatment = false).cache

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
  object Ad3rDaoMock extends Ad3rDAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._
      List(
        ("AD3R", "1", "07/02/2017", "000002354", "0", "25164719", "null",
          "/mnt/isilonshare_gas/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01016870329/2021/0205/00489490011_01016870329_202101_Ad3r_20210204224500_1_R.zip", "", "",
          "", "2020-07-20T05:57:36.479608", "1" ,"202101"),
        ("AD3R", "2", "null", "000000780", "null", "0016048901", "4408459",
          "/mnt/isilonshare_gas/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01016870329/2021/0205/00489490011_01016870329_202101_Ad3r_20210204224500_1_R.zip", "", "",
          "", "2021-03-03T03:30:05.332000", "1" ,"202012"),
        ("AD3R", "3", "12/03/2019", "000000780", "null", "56885353", "null",
          "/mnt/isilonshare_gas/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01016870329/2021/0205/00489490011_01016870329_202101_Ad3r_20210204224500_1_R.zip", "", "",
          "", "2021-03-03T03:30:05.332000", "1" ,"202012"),
        ("AD3R", "4", "", "000000048701.0", "0", "240990340061410052", "null",
          "/mnt/isilonshare_gas/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01016870329/2021/0205/00489490011_01016870329_202101_Ad3r_20210204224500_1_R.zip", "", "",
          "", "2020-07-09T07:35:25.323770", "1" ,"202012"),
        ("AD3R", "6", "   ", "00000875", "0", "ITGF030102119890", "null",
          "/mnt/isilonshare_gas/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_01016870329/2021/0205/00489490011_01016870329_202101_Ad3r_20210204224500_1_R.zip", "", "",
          "", "2020-11-06T15:00:44.589234", "1" ,"202012")

      ).toDF(
        Ad3rSchema.cod_flusso,
        Ad3rSchema.cod_pdr,
        Ad3rSchema.data_prest,
        Ad3rSchema.let_tot_prel,
        Ad3rSchema.let_tot_conv,
        Ad3rSchema.matr_mis,
        Ad3rSchema.matr_conv,
        Ad3rSchema.local_file,
        Ad3rSchema.ammissibilita,
        Ad3rSchema.piva_distr,
        Ad3rSchema.piva_utente,
        Ad3rSchema.d_caricamento,
        Ad3rSchema.mot_ret_lett,
        MeasureDAO.ANNO_MESE_COL_NAME
      )
    }
  }

}
