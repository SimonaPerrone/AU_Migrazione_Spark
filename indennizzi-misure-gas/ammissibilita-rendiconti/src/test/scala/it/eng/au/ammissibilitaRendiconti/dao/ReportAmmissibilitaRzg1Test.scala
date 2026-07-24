package it.eng.au.ammissibilitaRendiconti.dao

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.ammissibilitaRendiconti.model.ReportAmmissibilitaRzg1
import it.eng.au.ammissibilitaRendiconti.schema.ReportAmmissibilitaRzg1Schema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.junit.Assert

import java.io.File
import java.sql.Timestamp

class ReportAmmissibilitaRzg1Test extends EnvironmentSparkTest {
  def testGetAlreadyComputedZips(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val reportAmmissibilitaRzg1DAOMock = new ReportAmmissibilitaRzg1DAOMock
    val df = reportAmmissibilitaRzg1DAOMock.readTable

    val result = reportAmmissibilitaRzg1DAOMock.getAlreadyComputedZips(df)
      .toDF

    result.show
    Assert.assertEquals(8, result.count)
    Assert.assertEquals(2, result.columns.length)
  }

  def testGetAlreadyTransmitted(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val reportAmmissibilitaRzg1DAOMock = new ReportAmmissibilitaRzg1DAOMock
    val df = reportAmmissibilitaRzg1DAOMock.readTable

    val result = reportAmmissibilitaRzg1DAOMock.getAlreadyTransmittedZips(df)
      .toDF(ReportAmmissibilitaRzg1Schema.zip_file_name)
    result.show

    Assert.assertEquals(6, result.count)
    Assert.assertEquals(1, result.columns.length)
  }

  def testGetAmmissibilita(): Unit = {
    val reportAmmissibilitaRzg1DAOMock = new ReportAmmissibilitaRzg1DAOMock
    FileUtils.deleteDirectory(new File(reportAmmissibilitaRzg1DAOMock.parquetPath + "/"))

    val reportAmmissibilitaRzg1 = Environment.sparkContext.parallelize(Seq(
      ReportAmmissibilitaRzg1("cartella1", "zip1", 1666881L, "csv1", "cartella_amm1", "ammissibilita1", ammissibilita = true, "", "", Timestamp.valueOf("2022-10-27 15:31:00"), "202210"),
      ReportAmmissibilitaRzg1("cartella2", "zip2", 1666882L, "csv2", "cartella_amm2", "ammissibilita2", ammissibilita = true, "", "", Timestamp.valueOf("2022-10-27 15:32:00"), "202210"),
      ReportAmmissibilitaRzg1("cartella3", "zip3", 1666883L, "csv3", "cartella_amm3", "ammissibilita3", ammissibilita = false, "1", "error", Timestamp.valueOf("2022-10-27 15:33:00"), "202210"),
      ReportAmmissibilitaRzg1("cartella4", "zip4", 1666884L, "csv4", "cartella_amm4", "ammissibilita4", ammissibilita = false, "1", "error", Timestamp.valueOf("2022-10-27 15:34:00"), "202210")
    ))

    val result = reportAmmissibilitaRzg1DAOMock.getReportAmmissibilitaOutput(reportAmmissibilitaRzg1)
    result.show

    Assert.assertEquals(4, result.count)
    Assert.assertEquals(12, result.columns.length)
  }

  class ReportAmmissibilitaRzg1DAOMock extends ReportAmmissibilitaRzg1DAO {
    override val parquetPath: String = "src/test/resources/output/report_ammissibilita_rzg1"

    override def readTable: DataFrame = {
      val sqlContext = Environment.sqlContext
      import sqlContext.implicits._

      Environment.sparkContext.parallelize(Seq(
        ("cartella1", "zip1", 166681L, "csv1", "cartella_amm1", "file1", true, null, null, "2022-10-27 15:21:00", 12341L, "202207"),
        ("cartella2", "zip2", 166682L, "csv2", "cartella_amm2", "file2", true, null, null, "2022-10-27 15:22:00", 12342L, "202208"),
        ("cartella3", "zip3", 166683L, "csv3", "cartella_amm3", "file3", true, null, null, "2022-10-27 15:23:00", 12343L, "202209"),
        ("cartella4", "zip4", 166684L, "csv4", "cartella_amm4", "file4", true, null, null, "2022-10-27 15:24:00", 12344L, "202210"),
        ("cartella5", "zip1", 166685L, "csv1", "cartella_amm5", "file5", true, null, null, "2022-10-27 15:25:00", 12345L, "202210"),

        ("cartella6", "zip6", 166686L, "csv6", "cartella_amm6", "file6", false, null, null, "2022-10-27 15:26:00", 12346L, "202210"),
        ("cartella7", "zip7", 166687L, "csv7", "cartella_amm7", "file7", true, null, null, "2022-10-27 15:27:00", 54321L, "202211"),
        ("cartella8", "zip8", 166688L, "csv8", "cartella_amm8", "file7", true, null, null, "2022-10-27 15:28:00", 54321L, "202211")
      )).toDF(ReportAmmissibilitaRzg1Schema.getValues: _*)
    }
  }
}
