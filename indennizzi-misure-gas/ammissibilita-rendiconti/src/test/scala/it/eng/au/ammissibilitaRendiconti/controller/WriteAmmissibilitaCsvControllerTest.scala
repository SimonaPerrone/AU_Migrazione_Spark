package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.ammissibilitaRendiconti.model.{CsvRzg1Metadata, ZipRzg1Metadata}
import it.eng.au.ammissibilitaRendiconti.schema.AmmissibilitaCsvSchema
import it.eng.au.ammissibilitaRendiconti.utility.constants.Constants.{AMMISSIBILITA_SI_1, CSV_DELIMITER}
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.calcite.avatica.ConnectionConfigImpl.PropEnv
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.io.File
class WriteAmmissibilitaCsvControllerTest extends EnvironmentSparkTest {
  def testWriteCsv(): Unit = {
    val currentYear = Properties.getCurrentYear
    val currentMonth = Properties.getCurrentMonth
    val path = new File(Properties.getRzg1AmmOutputPath + "/CIG1_udd1/" + currentYear + "/" + currentMonth + "/")
    // Delete the directory
    FileUtils.deleteDirectory(path)
    // and recreate it
    path.mkdirs()

    val csvFile1 = CsvRzg1Metadata("csvFile1", "header", Some("07/11/2022"), Some("1"), Some("id1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("1.0"))
    val csvFile2 = CsvRzg1Metadata("csvFile2", "header", Some("07/11/2022"), Some("2"), Some("id1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("1.0"), Some("0.0"), Some("0.0"))
    val csvFile3 = CsvRzg1Metadata("csvFile3", "header", Some("07/11/2022"), Some("3"), Some("id1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("2.0"), Some("1.0"), Some("0.0"))
    val csvFile4 = CsvRzg1Metadata("csvFile4", "header", Some("07/11/2022"), Some("4"), Some("id1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("3.0"), Some("2.0"), Some("0.0"))

    val zipMetaData = Environment.sparkContext.parallelize(Seq(
      ZipRzg1Metadata(new File("src/test/resources/input/write_csv/sample1_1.zip"), 123L, Some(csvFile1), "udd1", "id1", "udd1", "2022", "11", "202211", "20221107153100", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("src/test/resources/input/write_csv/sample1_2.zip"), 123L, Some(csvFile1), "udd1", "id1", "udd1", "2022", "11", "202211", "20221107153100", "2", isAmmissibile = true),
      ZipRzg1Metadata(new File("src/test/resources/input/write_csv/sample2.zip"), 123L, Some(csvFile2), "udd1", "id1", "udd1", "2022", "11", "202211", "20221107153200", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("src/test/resources/input/write_csv/sample3.zip"), 123L, Some(csvFile3), "udd1", "id1", "udd1", "2022", "11", "202211", "20221107153300", "1", isAmmissibile = false, "505", "not supported"),
      ZipRzg1Metadata(new File("src/test/resources/input/write_csv/sample4.zip"), 123L, Some(csvFile4), "udd2", "id2", "udd2", "2022", "11", "202211", "20221107153400", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("src/test/resources/input/write_csv/sample5.zip"), 123L, None, "udd1", "id1", "udd1", "2022", "11", "202211", "20221107153500", "1", isAmmissibile = false, "404", "not found")
    ))

    val result = WriteAmmissibilitaCsvController.writeCsv(zipMetaData)
      .cache
    result.collect.foreach(println)

    Assert.assertEquals(6, result.count)
    Assert.assertEquals(4, result.filter(_.ammissibilita == true).count)
    Assert.assertEquals(1, result.filter(_.cartella_cloud_ammissibilita.contains("Couldn't")).count)

    val ammissibilitaCsv = Environment.spark
      .read
      .option("header", value = true)
      .option("delimiter", CSV_DELIMITER)
      .csv(Properties.getRzg1AmmOutputPath + "/CIG1_udd1/" + currentYear + "/" + currentMonth + "/*.csv")

    ammissibilitaCsv.show

    Assert.assertEquals(5, ammissibilitaCsv.count)
    Assert.assertEquals(4, ammissibilitaCsv.columns.length)
    Assert.assertEquals(3, ammissibilitaCsv.where(col(AmmissibilitaCsvSchema.VERIFICA_AMM) === AMMISSIBILITA_SI_1).count)
  }
}